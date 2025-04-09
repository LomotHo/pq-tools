package cmd

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
)

type SchemaField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Optional bool   `json:"optional,omitempty"`
}

// 定义固定的数据结构，用于生成测试数据
type ParquetData struct {
	ID     int64   `parquet:"name=id"`
	Name   string  `parquet:"name=name"`
	Age    int32   `parquet:"name=age"`
	Active bool    `parquet:"name=active"`
	Weight float32 `parquet:"name=weight"`
}

var generateCmd = &cobra.Command{
	Use:   "generate [output]",
	Short: "生成测试用的Parquet文件",
	Long:  `生成测试用的Parquet文件，可以指定行数和自定义Schema`,
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		// 初始化随机数生成器
		rand.Seed(time.Now().UnixNano())

		// 获取输出路径
		outputPath := "./.tmp/test.parquet"
		if len(args) > 0 {
			outputPath = args[0]
		}

		// 确保输出目录存在
		dir := filepath.Dir(outputPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Printf("创建目录失败: %v\n", err)
			return
		}

		// 获取标志值
		rowCount, _ := cmd.Flags().GetInt("rows")
		
		// 创建输出文件
		file, err := os.Create(outputPath)
		if err != nil {
			fmt.Printf("创建文件失败: %v\n", err)
			return
		}
		defer file.Close()
		
		// 创建 writer
		writer := parquet.NewWriter(file)
		
		// 生成并写入数据
		for i := 0; i < rowCount; i++ {
			data := ParquetData{
				ID:     int64(i),
				Name:   fmt.Sprintf("用户%d", i),
				Age:    int32(20 + rand.Intn(50)),
				Active: rand.Intn(2) == 1,
				Weight: float32(50 + rand.Intn(50)),
			}
			
			if err := writer.Write(&data); err != nil {
				fmt.Printf("写入数据失败: %v\n", err)
				return
			}
		}
		
		// 关闭 writer
		if err := writer.Close(); err != nil {
			fmt.Printf("关闭 writer 失败: %v\n", err)
			return
		}
		
		fmt.Printf("成功生成 %d 行数据到 %s\n", rowCount, outputPath)
	},
}

func init() {
	rootCmd.AddCommand(generateCmd)
	generateCmd.Flags().IntP("rows", "r", 100, "生成的行数")
	generateCmd.Flags().StringP("schema", "s", "", "自定义Schema，JSON格式（当前不支持）")
} 