mod structs;
use clap::Parser;
use std::path::PathBuf;
use std::{error::Error, process::Command};
use structs::Schema;

#[derive(Parser)]
#[command(about = "PostgreSQL schema dump and organize", long_about = None)]
struct Args {
    #[arg(short, long)]
    db_url: String,

    #[arg(short, long)]
    output_fp: PathBuf,
}

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
    let args = Args::parse();

    let output = get_dump(&args.db_url)?;
    let schema = output.parse::<Schema>()?;
    schema.write_to_fs(&args.output_fp)?;
    Ok(())
}
