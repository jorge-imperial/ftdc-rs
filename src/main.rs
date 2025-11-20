use clap::Parser;
use regex::Regex;
use std::fs;
use std::path::Path;
use std::time::SystemTime;
use ftdc_rs::FTDCFileDecoder;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
//#[command(version, about, long_about = None)]
struct Args {
    /// Path to file(s) to convert.
    #[arg(short, long)]
    path: String,

    /// Path where the output files will be written.
    #[arg(short, long, default_value = "./")]
    output_path: String,

    /// Display additional information
    #[arg(short, long, required = false)]
    info: bool,

    /// Write the metrics in the file(s) to a JSON
    #[arg(short, long, required = false)]
    metadata: bool,

    ///  Filter metrics
    #[arg(short,long, default_value = None, required = false)]
    filter: Option<String>,
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

    // Check if there is a filter
    let mut filter: Option<Regex> = None;
    if args.filter.is_some() {
        let filter_str = args.filter.unwrap();

        match Regex::new(filter_str.as_str()) {
            Ok(r) => filter = Some(r),
            Err(e) => {
                println!("Error parsing regular expression {}: {}", filter_str, e);
                return Ok(());
            }
        }
    }

    for file_name in files_to_parse {
        // Display additional information if info parameter is true
        if args.info {
            if let Err(e) = display_file_info(&file_name) {
                println!("Error displaying file info: {}", e);
            }
        }

        let full_path = Path::new(&file_name).file_name().unwrap().to_str().unwrap(); // <- living dangerously
        let out_file_name = format!("{}{}.parquet", args.output_path, full_path);

        println!("Processing {file_name} -> {out_file_name}");

        // optional metadata output
        let metadata;
        let metadata_file_path: String;
        if args.metadata {
            metadata_file_path = format!("{}_metadata.json", out_file_name);
            metadata = Some(metadata_file_path.as_str());
        } else {
            metadata = None;
        }

        match FTDCFileDecoder::write_to_parquet(
            file_name.as_str(),
            out_file_name.as_str(),
            metadata,
            filter.clone(),
        ) {
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
