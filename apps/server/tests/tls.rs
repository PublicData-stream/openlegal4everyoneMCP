//! In-memory TLS regression for RUSTSEC-2026-0285; no network or retained keys.
//! Uses the same resolved Rustls package as WebTransport, reqwest, and SQLx.

use std::io::{Cursor, Read, Write};
use std::sync::Arc;
use wtransport::tls::rustls::{self, Connection, crypto::CryptoProvider};

fn providers() -> [CryptoProvider; 2] {
    // Both providers are enabled in the workspace's single supported feature set.
    [
        rustls::crypto::ring::default_provider(),
        rustls::crypto::aws_lc_rs::default_provider(),
    ]
}

fn pair(provider: CryptoProvider) -> (Connection, Connection) {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.der().clone()).unwrap();
    let provider = Arc::new(provider);
    let client = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(signing_key.serialize_der()).into(),
        )
        .unwrap();
    (
        rustls::ClientConnection::new(Arc::new(client), "localhost".try_into().unwrap())
            .unwrap()
            .into(),
        rustls::ServerConnection::new(Arc::new(server))
            .unwrap()
            .into(),
    )
}

fn output(connection: &mut Connection) -> Vec<u8> {
    let mut bytes = Vec::new();
    for _ in 0..16 {
        if !connection.wants_write() {
            return bytes;
        }
        assert!(connection.write_tls(&mut bytes).unwrap() > 0);
    }
    panic!("fixture exceeded the TLS output budget");
}

fn receive(connection: &mut Connection, bytes: &[u8]) -> Result<(), rustls::Error> {
    assert_eq!(
        connection.read_tls(&mut Cursor::new(bytes)).unwrap(),
        bytes.len()
    );
    connection.process_new_packets().map(|_| ())
}

fn rejects_message_across_key_change(extra: &[u8]) {
    for provider in providers() {
        let (mut client, mut server) = pair(provider);
        receive(&mut server, &output(&mut client)).unwrap();
        let flight = output(&mut server);
        // TLS record header: content type, legacy version, two-byte payload length.
        assert_eq!(flight[0], 22); // Handshake
        let length = usize::from(u16::from_be_bytes([flight[3], flight[4]]));
        let mut record = flight[..5 + length].to_vec();
        assert_eq!(record[5], 2); // ServerHello
        let handshake_length =
            (usize::from(record[6]) << 16) | (usize::from(record[7]) << 8) | usize::from(record[8]);
        assert_eq!(handshake_length + 4, length);
        record.extend_from_slice(extra);
        record[3..5].copy_from_slice(&u16::try_from(length + extra.len()).unwrap().to_be_bytes());
        assert_eq!(
            receive(&mut client, &record),
            Err(rustls::Error::PeerMisbehaved(
                rustls::PeerMisbehaved::KeyEpochWithPendingFragment
            )),
            "plaintext handshake data must not follow ServerHello in the same record"
        );
    }
}

#[test]
fn complete_plaintext_handshake_after_server_hello_is_rejected() {
    // EncryptedExtensions with an empty extension vector, incorrectly in plaintext.
    rejects_message_across_key_change(&[8, 0, 0, 2, 0, 0]);
}

#[test]
fn partial_plaintext_handshake_after_server_hello_is_rejected() {
    rejects_message_across_key_change(&[8, 0, 0]);
}

#[test]
fn correctly_separated_tls13_handshake_and_application_data_succeed() {
    for provider in providers() {
        let (mut client, mut server) = pair(provider);
        for _ in 0..8 {
            receive(&mut server, &output(&mut client)).unwrap();
            receive(&mut client, &output(&mut server)).unwrap();
            if !client.is_handshaking() && !server.is_handshaking() {
                break;
            }
        }
        assert!(!client.is_handshaking() && !server.is_handshaking());
        assert_eq!(
            client.protocol_version(),
            Some(rustls::ProtocolVersion::TLSv1_3)
        );
        client.writer().write_all(b"synthetic control").unwrap();
        receive(&mut server, &output(&mut client)).unwrap();
        let mut received = [0; 17];
        server.reader().read_exact(&mut received).unwrap();
        assert_eq!(&received, b"synthetic control");
    }
}
