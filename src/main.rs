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

    for f in files_to_parse {
        // Display additional information if info parameter is true
        if args.info {
            if let Err(e) = display_file_info(&f) {
                println!("Error displaying file info: {}", e);
            }
        }

        // TODO: This can be parallelized per file
        let mut file_parser = FTDCFileDecoder::new();

        // Use read_and_decode instead of read_file to get the decoded data
        let result = file_parser.read_and_decode(&*f);
        match result {
            Ok(metrics) => {
                if args.info {
                    println!("Successfully processed file: {}", f);
                    println!("Number of metrics: {}", metrics.len());
                    println!("-----------------------------------");
                }

                // Get the output file path
                let file_name = Path::new(&f)
                    .file_name()
                    .unwrap_or_else(|| std::ffi::OsStr::new("output"));
                let mut output_path = Path::new(&args.output).to_path_buf();

                // Write the decoded data to the specified output format
                if args.format == "json" {
                    // Write to JSON format
                    output_path.push(format!("{}.json", file_name.to_string_lossy()));

                    // Convert metrics to JSON
                    let json = serde_json::to_string_pretty(
                        &metrics
                            .iter()
                            .map(|m| {
                                serde_json::json!({
                                    "name": m.name,
                                    "data": m.data
                                })
                            })
                            .collect::<Vec<_>>(),
                    )
                    .unwrap();

                    // Write JSON to file
                    fs::write(&output_path, json)?;

                    if args.info {
                        println!("Wrote JSON output to: {}", output_path.display());
                    }
                } else {
                    // Write to Parquet format
                    output_path.push(format!("{}.parquet", file_name.to_string_lossy()));

                    // TODO: Implement Parquet output
                    // For now, just write a placeholder message
                    println!(
                        "Parquet output not yet implemented. Would write to: {}",
                        output_path.display()
                    );
                }
            }
            Err(error) => {
                println!("{error:?}")
            }
        }
    }

    Ok(())
}
