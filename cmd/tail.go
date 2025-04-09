package cmd

import (
	"fmt"
	"os"
	"github.com/LomotHo/pq-tools/pkg/parquet"
	"strconv"

	"github.com/spf13/cobra"
)

// tailCmd represents the tail command
var tailCmd = &cobra.Command{
	Use:   "tail [file]",
	Short: "Display the last few rows of a Parquet file",
	Long:  `Display the last few rows of a Parquet file, default is 10 rows.`,
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]
		
		// Get the number of rows parameter
		nStr, _ := cmd.Flags().GetString("n")
		n, err := strconv.Atoi(nStr)
		if err != nil {
			n = 10 // If parsing fails, display 10 rows by default
		}
		
		// Get formatting option
		pretty, _ := cmd.Flags().GetBool("pretty")

		// Create Parquet reader
		reader, err := handleParquetReader(filePath)
		if err != nil {
			er(err.Error())
			return
		}
		defer safeClose(reader)

		// Read the last n rows
		rows, err := reader.Tail(n)
		if err != nil {
			er(handleRowsError(err).Error())
			return
		}

		// Print the results
		if err := parquet.PrintJSON(rows, os.Stdout, pretty); err != nil {
			er(fmt.Sprintf("Failed to print data: %v", err))
		}
	},
}

func init() {
	rootCmd.AddCommand(tailCmd)
	tailCmd.Flags().StringP("n", "n", "10", "Number of rows to display")
	tailCmd.Flags().BoolP("pretty", "p", false, "Use formatted output (multiple lines per record)")
} 