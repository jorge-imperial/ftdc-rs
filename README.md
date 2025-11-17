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

