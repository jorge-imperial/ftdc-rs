# ftdc-rs
Tools for analysis of MongoDB logs and diagnostics.

Reads MongoDB FTDC diagnostics files and converts them to Parquet for analysis.


```
Usage: ftdc [OPTIONS] --path <PATH>

Options:
  -p, --path <PATH>      Path to file(s) to convert
      --prefix <PREFIX>  Path where the output files will be written [default: ./]
  -i, --info             Display additional information
  -m, --metadata         Write the metrics in the file(s) to a JSON
  -h, --help             Print help
  -V, --version          Print version
  ```

## Example

To decode and convert files in the `mongo-diag-0` directory and write to the current directory, writing metadata for each file to a JSON:
```
ftdc --path ./mongo-diag-0  --prefix ./ --metadata                                                                                  ─╯
Processing ./mongo-diag-0/metrics.2025-11-16T17-21-08Z-00000 -> ./metrics.2025-11-16T17-21-08Z-00000.parquet
Successfully written to ./metrics.2025-11-16T17-21-08Z-00000.parquet
Processing ./mongo-diag-0/metrics.2025-11-15T21-17-12Z-00000 -> ./metrics.2025-11-15T21-17-12Z-00000.parquet
Successfully written to ./metrics.2025-11-15T21-17-12Z-00000.parquet
Processing ./mongo-diag-0/metrics.interim -> ./metrics.interim.parquet
Successfully written to ./metrics.interim.parquet
```
