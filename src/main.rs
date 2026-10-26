use clap::Parser;
use ftdc_rs::Archive;
use regex::Regex;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::SystemTime;

#[derive(Parser, Debug)]
#[command(
    about = "Convert MongoDB FTDC diagnostic archives to Parquet",
    long_about = None
)]
struct Args {
    /// FTDC metrics file, or a directory of them.
    ///
    /// A directory is scanned for files whose names start with `metrics.`
    /// (`metrics.interim`, `metrics.<utc>-<seq>`). Other names are left
    /// alone. The matches are processed in name order. A path that is not
    /// a directory is that one file, whatever it is called.
    #[arg(short, long)]
    path: PathBuf,

    /// Directory the Parquet files are written into.
    #[arg(short, long, default_value = "./")]
    output_path: PathBuf,

    /// Print the size and modification time of each input.
    #[arg(short, long)]
    info: bool,

    /// Also write `<name>.metadata.json` next to each Parquet file.
    ///
    /// The sidecar is relaxed extended JSON: every type 0 wrapper
    /// (`_id` and `doc`) and every type 2 wrapper (`_id`, `count`, and
    /// `doc`), in file order.
    #[arg(short, long)]
    metadata: bool,

    /// Keep metrics whose name matches this regular expression.
    #[arg(short, long)]
    filter: Option<String>,
}

/// Files this invocation will decode.
///
/// Directory entries are filtered to `metrics.*` and sorted. Anything that
/// is not a regular file (a subdirectory, a symlink to a directory) is
/// skipped. Diagnostic directories contain those, and they are not archives.
fn files_to_read(path: &Path) -> io::Result<Vec<PathBuf>> {
    if !path.is_dir() {
        return Ok(vec![path.to_path_buf()]);
    }

    let mut files = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let candidate = entry.path();
        if !candidate.is_file() {
            continue;
        }
        let Some(name) = candidate.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with("metrics.") {
            files.push(candidate);
        }
    }
    files.sort();
    Ok(files)
}

/// `<output>/<file>.parquet` for an input path.
///
/// `Path::join` inserts the separator. Concatenating the directory string
/// with the file name turned `--output-path ./out` into `./outmetrics...`.
fn parquet_output(dir: &Path, input: &Path) -> Option<PathBuf> {
    let name = input.file_name()?.to_str()?;
    Some(dir.join(format!("{name}.parquet")))
}

/// `<output>/<file>.metadata.json`, beside the Parquet file.
///
/// The previous name was `<file>.parquet_metadata.json`, glued onto the
/// Parquet path. The sidecar describes the archive, so it shares the
/// archive's name and uses a `.metadata.json` suffix.
fn metadata_output(parquet: &Path) -> PathBuf {
    let name = parquet
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("metrics.parquet");
    let stem = name.strip_suffix(".parquet").unwrap_or(name);
    parquet.with_file_name(format!("{stem}.metadata.json"))
}

fn display_file_info(file_path: &Path) -> io::Result<()> {
    println!("File: {}", file_path.display());

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

fn main() -> ExitCode {
    // `warn` is the default so a skipped chunk type is visible without
    // RUST_LOG. `error` is a file that failed. A set RUST_LOG still wins.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();

    let args = Args::parse();

    let files = match files_to_read(&args.path) {
        Ok(files) => files,
        Err(error) => {
            log::error!("failed to read {}: {error}", args.path.display());
            return ExitCode::FAILURE;
        }
    };
    if files.is_empty() {
        log::error!("no metrics.* files in {}", args.path.display());
        return ExitCode::FAILURE;
    }

    let filter = match args.filter.as_deref().map(Regex::new).transpose() {
        Ok(filter) => filter,
        Err(error) => {
            log::error!(
                "invalid --filter {}: {error}",
                args.filter.as_deref().unwrap_or("")
            );
            return ExitCode::FAILURE;
        }
    };

    let mut failed = false;
    for file_name in &files {
        if args.info
            && let Err(error) = display_file_info(file_name)
        {
            log::error!("{error}");
            failed = true;
        }

        let Some(out_file) = parquet_output(&args.output_path, file_name) else {
            log::error!("input path {} has no UTF-8 file name", file_name.display());
            failed = true;
            continue;
        };
        let metadata = args.metadata.then(|| metadata_output(&out_file));

        println!(
            "Processing {} -> {}",
            file_name.display(),
            out_file.display()
        );

        match Archive::write_to_parquet(file_name, &out_file, metadata.as_deref(), |name| {
            filter
                .as_ref()
                .map(|expression| expression.is_match(name))
                .unwrap_or(true)
        }) {
            Ok(()) => println!("Successfully written to {}", out_file.display()),
            Err(error) => {
                log::error!("failed to write {}: {error}", out_file.display());
                failed = true;
            }
        }
    }

    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use super::{files_to_read, metadata_output, parquet_output};
    use std::path::Path;

    #[test]
    fn directory_scan_keeps_metrics_files_in_name_order() {
        let dir = std::env::temp_dir().join(format!("ftdc-rs-cli-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("metrics.b"), b"").unwrap();
        std::fs::write(dir.join("metrics.a"), b"").unwrap();
        std::fs::write(dir.join("metrics.interim"), b"").unwrap();
        std::fs::write(dir.join("notes.txt"), b"").unwrap();
        std::fs::create_dir(dir.join("nested")).unwrap();

        let files = files_to_read(&dir).unwrap();
        let names: Vec<_> = files
            .iter()
            .map(|path| path.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        assert_eq!(
            names,
            vec![
                "metrics.a".to_string(),
                "metrics.b".to_string(),
                "metrics.interim".to_string()
            ]
        );

        // A path to one file is that file, even when the name would be
        // skipped inside a directory scan.
        let one = files_to_read(&dir.join("notes.txt")).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].file_name().unwrap(), "notes.txt");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn output_paths_join_the_directory_and_use_a_metadata_suffix() {
        let parquet = parquet_output(Path::new("out"), Path::new("diag/metrics.interim")).unwrap();
        assert_eq!(parquet, Path::new("out/metrics.interim.parquet"));
        assert_eq!(
            metadata_output(&parquet),
            Path::new("out/metrics.interim.metadata.json")
        );
    }
}
