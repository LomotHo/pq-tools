# PQ-Tools

PQ-Tools is a simple and easy-to-use Parquet file processing toolkit that allows you to work with Parquet files just like JSONL files.

Supports complex nested schemas including structs, lists, maps, and files written by all major Parquet writers (pyarrow, Spark, parquet-cpp-arrow v23+, etc.).

## Features

- `pq head` - Display the first few rows of a Parquet file
- `pq tail` - Display the last few rows of a Parquet file
- `pq cat` - Stream all rows in a Parquet file (memory-efficient)
- `pq sample` - Randomly sample rows from a Parquet file
- `pq wc` - Count the number of rows in a Parquet file
- `pq schema` - Display the schema of a Parquet file
- `pq split` - Split a Parquet file into multiple smaller files
- `pq generate` - Generate a test Parquet file
- `pq version` - Display version information

## Installation

### From GitHub Releases

Download the latest binary for your platform from [Releases](https://github.com/LomotHo/pq-tools/releases).

```bash
# Example: Linux amd64
wget https://github.com/LomotHo/pq-tools/releases/latest/download/pq-linux-amd64.tar.gz
tar xzf pq-linux-amd64.tar.gz
sudo mv pq /usr/local/bin/
```

### Build from source

```bash
git clone https://github.com/LomotHo/pq-tools.git
cd pq-tools
go build -o pq
sudo mv pq /usr/local/bin/
```

## Usage

### Display the first few rows

```bash
# Display the first 10 rows by default
pq head data.parquet

# Display the first 5 rows
pq head -n 5 data.parquet

# Pretty print
pq head -n 5 -p data.parquet
```

### Display the last few rows

```bash
pq tail data.parquet
pq tail -n 5 data.parquet
```

### Display all rows (streaming)

```bash
# Stream all rows — constant memory usage
pq cat data.parquet

# Works with pipes
pq cat data.parquet | jq '.name' | head -20
```

### Random sampling

```bash
# Sample 10 random rows (default)
pq sample data.parquet

# Sample 50 random rows
pq sample -n 50 data.parquet

# Pretty print
pq sample -n 5 -p data.parquet
```

### Display schema

```bash
pq schema data.parquet
```

### Count rows

```bash
# Display row count and filename
pq wc data.parquet

# Display row count only
pq wc -l data.parquet
```

### Split files

```bash
# Split into 2 files by default
pq split data.parquet

# Split into 5 files
pq split -n 5 data.parquet
```

Split works with all schema types including nested structs, lists, and maps.

### Generate test files

```bash
# Generate a test file with default schema
pq generate output.parquet

# Generate with 1000 rows
pq generate output.parquet -r 1000
```

## Dependencies

- [cobra](https://github.com/spf13/cobra) - Command-line interface framework
- [parquet-go](https://github.com/LomotHo/parquet-go) - Parquet format processing library for Go (fork with thrift decoder fixes)
