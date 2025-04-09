package cmd

import (
	"fmt"
	"pq-tools/pkg/parquet"

	"github.com/spf13/cobra"
)

// schemaCmd 表示schema命令
var schemaCmd = &cobra.Command{
	Use:   "schema [file]",
	Short: "显示parquet文件的schema信息",
	Long:  `显示parquet文件的schema信息，包括字段名称、类型等。`,
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]
		
		// 创建parquet读取器
		reader, err := parquet.NewParquetReader(filePath)
		if err != nil {
			er(fmt.Sprintf("无法读取文件: %v", err))
			return
		}
		defer reader.Close()

		// 获取schema信息
		schema, err := reader.GetSchema()
		if err != nil {
			er(fmt.Sprintf("获取schema信息失败: %v", err))
			return
		}

		// 打印结果
		fmt.Printf("文件: %s 的Schema信息:\n\n%s\n", filePath, schema)
	},
}

func init() {
	rootCmd.AddCommand(schemaCmd)
} 