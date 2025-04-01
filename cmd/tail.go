package cmd

import (
	"fmt"
	"os"
	"pq-tools/pkg/parquet"
	"strconv"

	"github.com/spf13/cobra"
)

// tailCmd 表示tail命令
var tailCmd = &cobra.Command{
	Use:   "tail [file]",
	Short: "显示parquet文件的最后几行",
	Long:  `显示parquet文件的最后几行内容，默认显示10行。`,
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]
		
		// 获取行数参数
		nStr, _ := cmd.Flags().GetString("n")
		n, err := strconv.Atoi(nStr)
		if err != nil {
			n = 10 // 如果解析失败，默认显示10行
		}

		// 创建parquet读取器
		reader, err := parquet.NewParquetReader(filePath)
		if err != nil {
			er(fmt.Sprintf("无法读取文件: %v", err))
			return
		}
		defer reader.Close()

		// 读取最后n行
		rows, err := reader.Tail(n)
		if err != nil {
			er(fmt.Sprintf("读取数据失败: %v", err))
			return
		}

		// 打印结果
		if err := parquet.PrintJSON(rows, os.Stdout); err != nil {
			er(fmt.Sprintf("打印数据失败: %v", err))
		}
	},
}

func init() {
	rootCmd.AddCommand(tailCmd)
	tailCmd.Flags().StringP("n", "n", "10", "要显示的行数")
} 