//! Admission of explicitly provisioned, immutable Korean dictionary artifacts.
use mecab_ko_dict::{dictionary::SystemDictionary, matrix::Matrix};
use openlegal_domain::legal::DatabaseError as E;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, io::Read, path::Path};

pub const SOURCE_SHA256: &str = "702ced21c6167e9d9aebc674ab5ee54af58d4443975f2940d37d0567c020591a";
pub const SOURCE_ENTRY_COUNT: usize = 816_283;
pub const SOURCE_URL: &str = "https://lindera.dev/mecab-ko-dic-2.1.1-20180720.tar.gz";
pub const ARTIFACT_FILES: [&str; 5] =
    ["sys.dic", "matrix.bin", "entries.bin", "COPYING", "AUTHORS"];
const MAX_FILE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DictionaryManifest {
    pub format: String,
    pub source_sha256: String,
    pub builder: String,
    pub entry_count: usize,
    pub left_size: usize,
    pub right_size: usize,
    pub files: BTreeMap<String, String>,
}

/// Hash a bounded regular file. Dictionary artifacts are trusted operator inputs,
/// but missing, oversized or redirected files must never select another dictionary.
pub fn file_digest(path: &Path) -> Result<String, E> {
    let metadata = fs::symlink_metadata(path).map_err(|_| E::StorageUnavailable)?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_FILE_BYTES {
        return Err(E::StorageCorrupt);
    }
    let mut file = fs::File::open(path).map_err(|_| E::StorageUnavailable)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 65536];
    let mut total = 0_u64;
    loop {
        let n = file.read(&mut buffer).map_err(|_| E::StorageUnavailable)?;
        if n == 0 {
            break;
        }
        total += n as u64;
        if total > MAX_FILE_BYTES {
            return Err(E::StorageCorrupt);
        }
        digest.update(&buffer[..n]);
    }
    Ok(digest
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

/// Validate every source group and context identifier before publishing an artifact.
pub fn validate_dictionary(path: &Path) -> Result<(usize, usize, usize), E> {
    let dict = SystemDictionary::load(path).map_err(|_| E::StorageCorrupt)?;
    let count = dict.entry_count();
    let left = dict.matrix().left_size();
    let right = dict.matrix().right_size();
    // The fixed source is a full Korean dictionary. Tiny test dictionaries and
    // matrices lacking the default unknown-handler context IDs are not admitted.
    // Upstream names these dimensions left/right, but its dense addressing is
    // right_id + left_size * left_id: the source header is 3822 2693.
    if count != SOURCE_ENTRY_COUNT || left != 3822 || right != 2693 {
        return Err(E::StorageCorrupt);
    }
    let mut previous = String::new();
    for i in 0..count {
        let index = u32::try_from(i).map_err(|_| E::StorageCorrupt)?;
        let entry = dict.get_entry(index).map_err(|_| E::StorageCorrupt)?;
        if entry.surface.is_empty()
            || usize::from(entry.left_id) >= right
            || usize::from(entry.right_id) >= left
            || entry.feature.is_empty()
        {
            return Err(E::StorageCorrupt);
        }
        if entry.surface != previous {
            if entry.surface < previous || dict.trie().exact_match(&entry.surface) != Some(index) {
                return Err(E::StorageCorrupt);
            }
            previous.clone_from(&entry.surface);
        }
    }
    Ok((count, left, right))
}

/// Return a stable manifest identity after complete artifact validation.
pub fn admit(path: &Path) -> Result<String, E> {
    let manifest_path = path.join("manifest.json");
    let metadata = fs::symlink_metadata(&manifest_path).map_err(|_| E::StorageUnavailable)?;
    if !metadata.is_file() || metadata.len() > 16384 {
        return Err(E::StorageCorrupt);
    }
    let bytes = fs::read(manifest_path).map_err(|_| E::StorageUnavailable)?;
    let manifest: DictionaryManifest =
        serde_json::from_slice(&bytes).map_err(|_| E::StorageCorrupt)?;
    if manifest.format != "openlegal-mecab-mked-v1"
        || manifest.builder != "mecab-ko-dict-builder/0.7.2"
        || manifest.source_sha256 != SOURCE_SHA256
        || manifest.files.len() != ARTIFACT_FILES.len()
    {
        return Err(E::StorageCorrupt);
    }
    for name in ARTIFACT_FILES {
        if manifest.files.get(name) != Some(&file_digest(&path.join(name))?) {
            return Err(E::StorageCorrupt);
        }
    }
    let (count, left, right) = validate_dictionary(path)?;
    if (count, left, right)
        != (
            manifest.entry_count,
            manifest.left_size,
            manifest.right_size,
        )
    {
        return Err(E::StorageCorrupt);
    }
    let canonical = serde_json::to_vec(&manifest).map_err(|_| E::StorageCorrupt)?;
    Ok(Sha256::digest(canonical)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_corrupt_and_miniature_artifacts_fail_closed() {
        use mecab_ko_dict_builder::{
            DictionaryBuilder, builder::BuildConfig, csv_parser::Encoding,
        };
        let temp = tempfile::tempdir().unwrap();
        assert_eq!(admit(temp.path()).unwrap_err(), E::StorageUnavailable);
        fs::write(temp.path().join("manifest.json"), b"{}").unwrap();
        assert_eq!(admit(temp.path()).unwrap_err(), E::StorageCorrupt);
        let source = temp.path().join("source");
        let output = temp.path().join("output");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("test.csv"), "가,1,1,100,NNG,*,T,가,*,*,*,*\n").unwrap();
        fs::write(
            source.join("matrix.def"),
            "2 2\n0 0 0\n0 1 0\n1 0 0\n1 1 0\n",
        )
        .unwrap();
        DictionaryBuilder::new(BuildConfig {
            input_dir: source.to_str().unwrap().into(),
            output_dir: output.to_str().unwrap().into(),
            compression_level: 0,
            encoding: Encoding::Utf8,
            verbose: false,
        })
        .build()
        .unwrap();
        assert_eq!(validate_dictionary(&output).unwrap_err(), E::StorageCorrupt);
        fs::write(output.join("entries.bin"), b"corrupt").unwrap();
        assert_eq!(validate_dictionary(&output).unwrap_err(), E::StorageCorrupt);
    }
}
