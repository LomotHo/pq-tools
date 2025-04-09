package cmd

import (
	"fmt"
	"github.com/LomotHo/pq-tools/pkg/parquet"
	"strconv"

	"github.com/spf13/cobra"
)

// splitCmd represents the split command
var splitCmd = &cobra.Command{
	Use:   "split [file]",
	Short: "Split a Parquet file into multiple smaller files",
	Long:  `Split a Parquet file into multiple smaller files, similar to the split command.`,
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]
		
		// Get the number of files to split into
		nStr, _ := cmd.Flags().GetString("n")
		n, err := strconv.Atoi(nStr)
		if err != nil || n <= 0 {
			n = 2 // If parsing fails or the value is invalid, default to 2 files
		}

		// Execute the split
		if err := parquet.SplitParquetFile(filePath, n); err != nil {
			er(handleSplitError(err).Error())
			return
		}

		fmt.Printf("Successfully split file %s into %d files\n", filePath, n)
	},
}

func init() {
	rootCmd.AddCommand(splitCmd)
	splitCmd.Flags().StringP("n", "n", "2", "Number of files to split into")
} 