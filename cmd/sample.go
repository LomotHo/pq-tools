package cmd

import (
	"fmt"
	"os"
	"strconv"

	"github.com/LomotHo/pq-tools/pkg/parquet"
	"github.com/spf13/cobra"
)

var sampleCmd = &cobra.Command{
	Use:   "sample [file]",
	Short: "Randomly sample rows from a Parquet file",
	Long:  `Randomly sample rows from a Parquet file, default is 10 rows.`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		nStr, _ := cmd.Flags().GetString("n")
		n, err := strconv.Atoi(nStr)
		if err != nil {
			n = 10
		}

		pretty, _ := cmd.Flags().GetBool("pretty")

		reader, err := handleParquetReader(args[0])
		if err != nil {
			er(err.Error())
			return
		}
		defer safeClose(reader)

		rows, err := reader.Sample(n)
		if err != nil {
			er(handleRowsError(err).Error())
			return
		}

		if err := parquet.PrintJSON(rows, os.Stdout, pretty); err != nil {
			er(fmt.Sprintf("Failed to print data: %v", err))
		}
	},
}

func init() {
	rootCmd.AddCommand(sampleCmd)
	sampleCmd.Flags().StringP("n", "n", "10", "Number of rows to sample")
	sampleCmd.Flags().BoolP("pretty", "p", false, "Use formatted output (multiple lines per record)")
}
