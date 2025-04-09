package cmd

import (
	"fmt"
	"os"
	"pq-tools/pkg/parquet"
	"strconv"

	"github.com/spf13/cobra"
)

// headCmd 表示head命令
var headCmd = &cobra.Command{
	Use:   "head [file]",
	Short: "显示parquet文件的前几行",
	Long:  `显示parquet文件的前几行内容，默认显示10行。`,
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]
		
		// 获取行数参数
		nStr, _ := cmd.Flags().GetString("n")
		n, err := strconv.Atoi(nStr)
		if err != nil {
			n = 10 // 如果解析失败，默认显示10行
		}

		// 获取格式化选项
		pretty, _ := cmd.Flags().GetBool("pretty")

		// 创建parquet读取器
		reader, err := parquet.NewParquetReader(filePath)
		if err != nil {
			er(fmt.Sprintf("无法读取文件: %v", err))
			return
		}
		defer reader.Close()

		// 读取前n行
		rows, err := reader.Head(n)
		if err != nil {
			er(fmt.Sprintf("读取数据失败: %v", err))
			return
		}

		// 打印结果
		if err := parquet.PrintJSON(rows, os.Stdout, pretty); err != nil {
			er(fmt.Sprintf("打印数据失败: %v", err))
		}
	},
}

func init() {
	rootCmd.AddCommand(headCmd)
	headCmd.Flags().StringP("n", "n", "10", "要显示的行数")
	headCmd.Flags().BoolP("pretty", "p", false, "使用格式化输出（每条记录多行）")
} 