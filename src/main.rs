use clap::Parser;
use ftdc::FTDCFileDecoder;
use std::fs;
use std::path::Path;
use std::time::SystemTime;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to file(s) to convert.
    #[arg(short, long)]
    path: String,

    /// Path where the output files will be written.
    #[arg(short, long, default_value = "./")]
    output: String,

    /// Display additional information
    #[arg(short, long)]
    info: bool,

    /// Output format (json or parquet)
    #[arg(short, long, default_value = "parquet")]
    format: String,
}

/// Displays additional information about a file when the info parameter is true
fn display_file_info(file_path: &str) -> std::io::Result<()> {
    println!("File: {}", file_path);

    let metadata = fs::metadata(file_path)?;
    println!("Size: {} bytes", metadata.len());

    if let Ok(modified) = metadata.modified() {
        if let Ok(duration) = modified.duration_since(SystemTime::UNIX_EPOCH) {
            println!("Last modified: {} seconds since epoch", duration.as_secs());
        }
    }

    println!("-----------------------------------");
    Ok(())
}

fn main() -> std::io::Result<()> {
    env_logger::init();

    let args = Args::parse();

    // Validate the format parameter
    if args.format != "json" && args.format != "parquet" {
        println!("Error: format must be either 'json' or 'parquet'");
        return Ok(());
    }

    let mut files_to_parse: Vec<String> = Vec::new();

    // Is this a directory?
    let path_directory = Path::new(&args.path);
    if path_directory.is_dir() {
        for entry in fs::read_dir(path_directory)? {
            let entry = entry?;
            files_to_parse.push(entry.path().display().to_string());
        }
    } else {
        files_to_parse.push(path_directory.display().to_string());
    }

    for file_name in files_to_parse {
        // Display additional information if info parameter is true
        if args.info {
            if let Err(e) = display_file_info(&file_name) {
                println!("Error displaying file info: {}", e);
            }
        }

        let outf = Path::new(&file_name).file_name().unwrap().to_str().unwrap();
        let out_file_name = format!("{}{}.parquet", args.output, outf);

        println!("Processing {file_name} -> {out_file_name}");

        match FTDCFileDecoder::write_to_parquet(&*file_name, &*out_file_name) {
            Ok(_) => {
                println!("Successfully written to {}", out_file_name);
            }
            Err(_e) => {
                println!("Failed to write to {}", out_file_name);
            }
        }
    }

    Ok(())
}
