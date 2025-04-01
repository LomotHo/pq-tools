package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "pq",
	Short: "pq is a tool for working with Parquet files",
	Long: `一个简单易用的Parquet文件处理工具集，
使得您可以像处理jsonl文件一样处理Parquet文件。
支持查看头部数据、尾部数据、计算行数以及拆分文件等功能。`,
}

// Execute 执行根命令
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// 这里可以添加全局标志
}

func er(msg interface{}) {
	fmt.Println("错误:", msg)
	os.Exit(1)
} 