package cmd

import (
	"fmt"
	"github.com/LomotHo/pq-tools/pkg/parquet"

	"github.com/spf13/cobra"
)

// wcCmd represents the wc command
var wcCmd = &cobra.Command{
	Use:   "wc [file]",
	Short: "Count the number of rows in a Parquet file",
	Long:  `Count the number of rows in a Parquet file, similar to the wc -l command.`,
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]
		
		// Check if we only need to display the line count
		linesOnly, _ := cmd.Flags().GetBool("l")

		// Create Parquet reader
		reader, err := parquet.NewParquetReader(filePath)
		if err != nil {
			er(fmt.Sprintf("Failed to read file: %v", err))
			return
		}
		defer reader.Close()

		// Get the row count
		count, err := reader.Count()
		if err != nil {
			er(fmt.Sprintf("Failed to count rows: %v", err))
			return
		}

		// Display results based on parameters
		if linesOnly {
			fmt.Println(count)
		} else {
			fmt.Printf("%d %s\n", count, filePath)
		}
	},
}

func init() {
	rootCmd.AddCommand(wcCmd)
	wcCmd.Flags().BoolP("l", "l", false, "Display only the line count")
} 