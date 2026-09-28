//! A verified edge certificate, rather than an HTTP header, selects tunnel admission.

use crate::{ServerError, config::EdgeMtlsConfig};
use rustls::{
    DigitallySignedStruct, DistinguishedName, Error, SignatureScheme,
    client::danger::HandshakeSignatureValid,
    pki_types::{CertificateDer, PrivateKeyDer, UnixTime, pem::PemObject},
    server::{
        WebPkiClientVerifier,
        danger::{ClientCertVerified, ClientCertVerifier},
    },
};
use std::{fmt, io::Read, path::Path, sync::Arc};
use wtransport::{Identity, tls::rustls};
use x509_parser::{extensions::GeneralName, parse_x509_certificate};

const MAX_CA_BYTES: u64 = 64 * 1024;

pub(crate) async fn server_identity(
    certificate: &Path,
    private_key: &Path,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), ServerError> {
    let identity = Identity::load_pemfiles(certificate, private_key).await?;
    let certificates = identity
        .certificate_chain()
        .as_slice()
        .iter()
        .map(|certificate| CertificateDer::from(certificate.der().to_vec()))
        .collect();
    let private_key = PrivateKeyDer::try_from(identity.private_key().secret_der().to_vec())?;
    Ok((certificates, private_key))
}

pub(crate) fn client_verifier(
    config: &EdgeMtlsConfig,
) -> Result<Arc<dyn ClientCertVerifier>, ServerError> {
    config.validate()?;
    let file = std::fs::File::open(&config.client_ca_file)?;
    let mut pem = Vec::new();
    file.take(MAX_CA_BYTES + 1).read_to_end(&mut pem)?;
    if pem.len() as u64 > MAX_CA_BYTES {
        return Err("edge client CA file exceeds limit".into());
    }
    let mut roots = rustls::RootCertStore::empty();
    let mut count = 0;
    for certificate in CertificateDer::pem_slice_iter(&pem) {
        count += 1;
        if count > 8 {
            return Err("too many edge client CA certificates".into());
        }
        roots.add(certificate?)?;
    }
    if count == 0 {
        return Err("edge client CA file has no certificates".into());
    }
    let chain_verifier = WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::ring::default_provider()),
    )
    .build()?;
    Ok(Arc::new(ExactDnsClientVerifier {
        chain_verifier,
        required_dns_san: config.required_client_dns_san.to_ascii_lowercase(),
    }))
}

struct ExactDnsClientVerifier {
    chain_verifier: Arc<dyn ClientCertVerifier>,
    required_dns_san: String,
}

impl fmt::Debug for ExactDnsClientVerifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExactDnsClientVerifier")
    }
}

impl ClientCertVerifier for ExactDnsClientVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.chain_verifier.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        let verified = self
            .chain_verifier
            .verify_client_cert(end_entity, intermediates, now)?;
        let (remaining, certificate) = parse_x509_certificate(end_entity.as_ref())
            .map_err(|_| Error::InvalidCertificate(rustls::CertificateError::BadEncoding))?;
        if !remaining.is_empty() {
            return Err(Error::InvalidCertificate(
                rustls::CertificateError::BadEncoding,
            ));
        }
        let san = certificate
            .subject_alternative_name()
            .map_err(|_| Error::InvalidCertificate(rustls::CertificateError::BadEncoding))?;
        let matches = san.is_some_and(|san| {
            san.value.general_names.iter().any(|name| {
                matches!(name, GeneralName::DNSName(value) if value.eq_ignore_ascii_case(&self.required_dns_san))
            })
        });
        if !matches {
            return Err(Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ));
        }
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.chain_verifier
            .verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.chain_verifier
            .verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.chain_verifier.supported_verify_schemes()
    }
}
