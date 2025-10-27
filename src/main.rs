mod structs;
use std::env::args;
use std::path::Path;
use std::{error::Error, process::Command};
use structs::Schema;

fn get_dump(db_url: &str) -> Result<String, Box<dyn Error>> {
    let output = Command::new("pg_dump").arg(db_url).arg("-s").output()?;
    if !output.stderr.is_empty() {
        return Err(format!(
            "pg_dump failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?)
}
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = args().collect();

    let db_url = &args[1];
    let base_dir = Path::new(&args[2]);
    println!("{db_url}");
    let output = get_dump(db_url)?;
    let schema = output.parse::<Schema>()?;
    schema.write_to_fs(base_dir)?;
    Ok(())
}
