package cmd

import (
	"fmt"
	"pq-tools/pkg/parquet"
	"strconv"

	"github.com/spf13/cobra"
)

// splitCmd 表示split命令
var splitCmd = &cobra.Command{
	Use:   "split [file]",
	Short: "将parquet文件拆分成多个小文件",
	Long:  `将parquet文件拆分成多个小文件，类似于split命令。`,
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]
		
		// 获取拆分文件数
		nStr, _ := cmd.Flags().GetString("n")
		n, err := strconv.Atoi(nStr)
		if err != nil || n <= 0 {
			n = 2 // 如果解析失败或值不合法，默认拆分成2个文件
		}

		// 执行拆分
		if err := parquet.SplitParquetFile(filePath, n); err != nil {
			er(fmt.Sprintf("拆分文件失败: %v", err))
			return
		}

		fmt.Printf("成功将文件 %s 拆分为 %d 个文件\n", filePath, n)
	},
}

func init() {
	rootCmd.AddCommand(splitCmd)
	splitCmd.Flags().StringP("n", "n", "2", "要拆分的文件数量")
} 