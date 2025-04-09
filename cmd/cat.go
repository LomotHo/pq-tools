package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"github.com/LomotHo/pq-tools/pkg/parquet"
	"github.com/spf13/cobra"
)

var catCmd = &cobra.Command{
	Use:   "cat [parquet file]",
	Short: "Print all rows in a parquet file",
	Long:  `Print all rows in a parquet file in a human-readable format.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// Set up signal handling, ignore SIGPIPE
		signal.Ignore(syscall.SIGPIPE)
		
		reader, err := handleParquetReader(args[0])
		if err != nil {
			return err
		}
		defer safeClose(reader)

		// Get total row count
		total, err := reader.Count()
		if err != nil {
			return handleRowsError(err)
		}

		rows, err := reader.Head(int(total))
		if err != nil {
			return handleRowsError(err)
		}

		// Use PrintJSON to print all rows, set pretty to false for single-line JSON
		err = parquet.PrintJSON(rows, os.Stdout, false)
		if err != nil {
			// Check for broken pipe error, if so, don't return error
			if pathErr, ok := err.(*os.PathError); ok && pathErr.Err == syscall.EPIPE {
				return nil
			}
			return fmt.Errorf("failed to print rows: %w", err)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(catCmd)
} 