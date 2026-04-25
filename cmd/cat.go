package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

var catCmd = &cobra.Command{
	Use:   "cat [parquet file]",
	Short: "Print all rows in a parquet file",
	Long:  `Print all rows in a parquet file in a human-readable format.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		signal.Ignore(syscall.SIGPIPE)

		reader, err := handleParquetReader(args[0])
		if err != nil {
			return err
		}
		defer safeClose(reader)

		encoder := json.NewEncoder(os.Stdout)
		err = reader.StreamAll(func(row map[string]interface{}) error {
			if err := encoder.Encode(row); err != nil {
				if pathErr, ok := err.(*os.PathError); ok && (pathErr.Err == syscall.EPIPE || pathErr.Err.Error() == "broken pipe") {
					return err
				}
				return fmt.Errorf("failed to print row: %w", err)
			}
			return nil
		})
		if err != nil {
			if pathErr, ok := err.(*os.PathError); ok && pathErr.Err == syscall.EPIPE {
				return nil
			}
			return err
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(catCmd)
}
