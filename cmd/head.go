package cmd

import (
	"fmt"
	"os"
	"strconv"
	"github.com/LomotHo/pq-tools/pkg/parquet"
	"github.com/spf13/cobra"
)

// headCmd represents the head command
var headCmd = &cobra.Command{
	Use:   "head [file]",
	Short: "Display the first few rows of a Parquet file",
	Long:  `Display the first few rows of a Parquet file, default is 10 rows.`,
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
		reader, err := parquet.NewParquetReader(filePath)
		if err != nil {
			er(fmt.Sprintf("Failed to read file: %v", err))
			return
		}
		defer reader.Close()

		// Read the first n rows
		rows, err := reader.Head(n)
		if err != nil {
			er(fmt.Sprintf("Failed to read data: %v", err))
			return
		}

		// Print the results
		if err := parquet.PrintJSON(rows, os.Stdout, pretty); err != nil {
			er(fmt.Sprintf("Failed to print data: %v", err))
		}
	},
}

func init() {
	rootCmd.AddCommand(headCmd)
	headCmd.Flags().StringP("n", "n", "10", "Number of rows to display")
	headCmd.Flags().BoolP("pretty", "p", false, "Use formatted output (multiple lines per record)")
} 