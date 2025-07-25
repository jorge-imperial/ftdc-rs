use clap::Parser;
use ftdc::FTDCFileDecoder;
use std::fs;
use std::path::Path;

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
}

fn main() -> std::io::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let mut files_to_parse: Vec<String> = Vec::new();

    // Is this a directory?
    let path_directory = Path::new(&args.path);
    if path_directory.is_dir() {
        for entry in fs::read_dir(path_directory)? {
            //println!("{}", entry?.path().display());
            let entry = entry?;
            files_to_parse.push(entry.path().display().to_string());
        }
    } else {
        files_to_parse.push(path_directory.display().to_string());
    }

    for f in files_to_parse {
        // TODO: This can be parallelized per file

        let mut file_parser = FTDCFileDecoder::new();

        let result = file_parser.read_file(&*f);
        match result {
            Ok(_parsed_file) => {
                //parsed_file.write_parquet(&*f).expect("TODO: panic message");
            }
            Err(error) => {
                println!("{error:?}")
            }
        }
    }

    Ok(())
}
