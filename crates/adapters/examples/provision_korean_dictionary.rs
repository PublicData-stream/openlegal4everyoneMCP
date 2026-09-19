//! Build a full dictionary from the checksum-verified archive extracted by the script.
use mecab_ko_dict_builder::{DictionaryBuilder, builder::BuildConfig, csv_parser::Encoding};
use openlegal_adapters::korean_dictionary::{
    self, ARTIFACT_FILES, DictionaryManifest, SOURCE_ENTRY_COUNT, SOURCE_SHA256,
};
use std::{collections::BTreeMap, error::Error, fs, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 4 {
        return Err("expected SOURCE_ARCHIVE EXTRACTED_SOURCE OUTPUT".into());
    }
    let archive = Path::new(&args[1]);
    let source = Path::new(&args[2]);
    let output = Path::new(&args[3]);
    if korean_dictionary::file_digest(archive)? != SOURCE_SHA256 {
        return Err("dictionary source digest mismatch".into());
    }
    if output.exists() {
        return Err("output directory must not exist".into());
    }
    let mut count = 0_usize;
    for entry in fs::read_dir(source)? {
        let path = entry?.path();
        if path.extension().is_some_and(|x| x == "csv") {
            let text = fs::read_to_string(path)?;
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .comment(Some(b'#'))
                .from_reader(text.as_bytes());
            for record in reader.records() {
                let record = record?;
                if record.len() != 12 || record[0].is_empty() {
                    return Err("incomplete source CSV record".into());
                }
                count += 1;
            }
        }
    }
    if count != SOURCE_ENTRY_COUNT {
        return Err("source dictionary is incomplete".into());
    }
    DictionaryBuilder::new(BuildConfig {
        input_dir: source.to_str().ok_or("UTF-8 source path required")?.into(),
        output_dir: output.to_str().ok_or("UTF-8 output path required")?.into(),
        compression_level: 0,
        encoding: Encoding::Utf8,
        verbose: true,
    })
    .build()?;
    let (entries, left_size, right_size) = korean_dictionary::validate_dictionary(output)?;
    if count != entries {
        return Err("builder omitted source records".into());
    }
    for name in ["COPYING", "AUTHORS"] {
        fs::copy(source.join(name), output.join(name))?;
    }
    let mut files = BTreeMap::new();
    for name in ARTIFACT_FILES {
        files.insert(
            name.to_owned(),
            korean_dictionary::file_digest(&output.join(name))?,
        );
    }
    let manifest = DictionaryManifest {
        format: "openlegal-mecab-mked-v1".into(),
        source_sha256: SOURCE_SHA256.into(),
        builder: "mecab-ko-dict-builder/0.7.2".into(),
        entry_count: entries,
        left_size,
        right_size,
        files,
    };
    fs::write(
        output.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    println!(
        "dictionary manifest {} ({} entries)",
        korean_dictionary::admit(output)?,
        entries
    );
    Ok(())
}
