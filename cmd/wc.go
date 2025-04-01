package cmd

import (
	"fmt"
	"pq-tools/pkg/parquet"

	"github.com/spf13/cobra"
)

// wcCmd 表示wc命令
var wcCmd = &cobra.Command{
	Use:   "wc [file]",
	Short: "计算parquet文件的行数",
	Long:  `计算parquet文件的行数，类似于wc -l命令。`,
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]
		
		// 检查是否只需要显示行数
		linesOnly, _ := cmd.Flags().GetBool("l")

		// 创建parquet读取器
		reader, err := parquet.NewParquetReader(filePath)
		if err != nil {
			er(fmt.Sprintf("无法读取文件: %v", err))
			return
		}
		defer reader.Close()

		// 获取行数
		count := reader.Count()

		// 根据参数显示结果
		if linesOnly {
			fmt.Println(count)
		} else {
			fmt.Printf("%d %s\n", count, filePath)
		}
	},
}

func init() {
	rootCmd.AddCommand(wcCmd)
	wcCmd.Flags().BoolP("l", "l", false, "只显示行数")
} 