package parquet

import (
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/parquet-go/parquet-go"
)

func SplitParquetFile(filePath string, numFiles int) error {
	srcFile, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %v", err)
	}
	defer srcFile.Close()

	header := make([]byte, 4)
	_, err = srcFile.Read(header)
	if err != nil {
		return fmt.Errorf("failed to read file header: %v", err)
	}
	_, err = srcFile.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to reset file position: %v", err)
	}
	if string(header) != "PAR1" {
		return fmt.Errorf("invalid file format: the file is not a valid Parquet file")
	}

	reader := parquet.NewReader(srcFile)
	if reader == nil {
		return fmt.Errorf("failed to create parquet reader")
	}
	defer reader.Close()

	totalRows := reader.NumRows()
	if totalRows == 0 {
		return fmt.Errorf("file contains no rows, no need to split")
	}

	rowsPerFile := int64(math.Ceil(float64(totalRows) / float64(numFiles)))

	baseName := filepath.Base(filePath)
	ext := filepath.Ext(baseName)
	baseName = strings.TrimSuffix(baseName, ext)
	dir := filepath.Dir(filePath)
	schema := reader.Schema()

	for i := 0; i < numFiles; i++ {
		startRow := int64(i) * rowsPerFile
		if startRow >= totalRows {
			break
		}
		endRow := startRow + rowsPerFile
		if endRow > totalRows {
			endRow = totalRows
		}

		outputPath := filepath.Join(dir, fmt.Sprintf("%s_%d%s", baseName, i+1, ext))
		outputFile, err := os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("failed to create output file %s: %v", outputPath, err)
		}

		if err := reader.SeekToRow(startRow); err != nil {
			outputFile.Close()
			return fmt.Errorf("failed to seek to row %d: %v", startRow, err)
		}

		writer := parquet.NewWriter(outputFile, schema)

		rowsToWrite := endRow - startRow
		rowCount := int64(0)
		batchSize := int64(100)
		rowBuf := make([]parquet.Row, batchSize)

		for rowCount < rowsToWrite {
			remaining := rowsToWrite - rowCount
			if remaining < batchSize {
				rowBuf = rowBuf[:remaining]
			}

			n, err := reader.ReadRows(rowBuf)
			if err != nil && err != io.EOF {
				writer.Close()
				outputFile.Close()
				return fmt.Errorf("failed to read rows: %v", err)
			}

			if n == 0 {
				break
			}

			if _, err := writer.WriteRows(rowBuf[:n]); err != nil {
				writer.Close()
				outputFile.Close()
				return fmt.Errorf("failed to write rows: %v", err)
			}

			rowCount += int64(n)
			if err == io.EOF {
				break
			}
		}

		if err := writer.Close(); err != nil {
			outputFile.Close()
			return fmt.Errorf("failed to close writer: %v", err)
		}
		if err := outputFile.Close(); err != nil {
			return fmt.Errorf("failed to close output file: %v", err)
		}
	}

	fmt.Printf("Successfully split file %s into %d files\n", filePath, numFiles)
	return nil
}
