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
		// 设置信号处理，忽略 SIGPIPE
		signal.Ignore(syscall.SIGPIPE)
		
		reader, err := parquet.NewParquetReader(args[0])
		if err != nil {
			return fmt.Errorf("failed to create parquet reader: %w", err)
		}
		defer reader.Close()

		// 获取总行数
		total, err := reader.Count()
		if err != nil {
			return fmt.Errorf("failed to get row count: %w", err)
		}

		rows, err := reader.Head(int(total))
		if err != nil {
			return fmt.Errorf("failed to read rows: %w", err)
		}

		// 使用 PrintJSON 打印所有行，设置 pretty 为 false，输出单行 JSON
		err = parquet.PrintJSON(rows, os.Stdout, false)
		if err != nil {
			// 检查是否是 broken pipe 错误，如果是则不返回错误
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