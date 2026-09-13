//! Test executable for the private cache protocol; never serves network requests.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().nth(1).as_deref() != Some("--cache-worker") {
        return Err("private worker mode required".into());
    }
    openlegal_adapters::persistent::run_worker()?;
    Ok(())
}
