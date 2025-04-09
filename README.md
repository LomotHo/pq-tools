# PQ-Tools

PQ-Tools is a simple and easy-to-use Parquet file processing toolkit that allows you to work with Parquet files just like JSONL files.

## Features

- `pq head` - Display the first few rows of a Parquet file
- `pq tail` - Display the last few rows of a Parquet file
- `pq cat` - Display all rows in a Parquet file
- `pq wc` - Count the number of rows in a Parquet file
- `pq split` - Split a Parquet file into multiple smaller files (**Note: Has known issues with INT64 encoding in some files**)
- `pq schema` - Display the schema of a Parquet file
- `pq generate` - Generate a test Parquet file with custom schema

## Installation

```bash
# Clone the repository
git clone https://github.com/lomotHo/pq-tools.git
cd pq-tools

# Compile and install
go build -o pq

# Move the executable to system path
sudo mv pq /usr/local/bin/
```

## Usage

### Display the first few rows

```bash
# Display the first 10 rows by default
pq head data.parquet

# Display the first 5 rows
pq head -n 5 data.parquet
```

### Display the last few rows

```bash
# Display the last 10 rows by default
pq tail data.parquet

# Display the last 5 rows
pq tail -n 5 data.parquet
```

### Display all rows

```bash
# Display all rows in the file
pq cat data.parquet
```

### Display schema

```bash
# Display the schema of a Parquet file
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

**Note: The split functionality has known issues with some Parquet files, particularly those containing INT64 fields.**

```bash
# Split into 2 files by default
pq split data.parquet

# Split into 5 files
pq split -n 5 data.parquet
```

### Generate test files

```bash
# Generate a test file with default schema
pq generate output.parquet

# Generate a test file with custom schema and 1000 rows
pq generate output.parquet -r 1000 -s '{"id":"INT64","name":"UTF8","age":"INT32","score":"DOUBLE","active":"BOOLEAN"}'
```

## Known Issues

1. **Split Command**: The `split` command may fail with an error "encoding parquet data page: encoding not supported for type INT64" when processing certain Parquet files with INT64 fields.

## Dependencies

- [cobra](https://github.com/spf13/cobra) - Command-line interface framework
- [parquet-go](https://github.com/parquet-go/parquet-go) - Parquet format processing library for Go