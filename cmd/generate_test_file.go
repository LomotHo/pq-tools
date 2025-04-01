package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

// generateCmd 表示generate命令，用于生成测试文件
var generateCmd = &cobra.Command{
	Use:   "generate [output_path]",
	Short: "生成用于测试的Parquet文件",
	Long:  `生成一个包含测试数据的Parquet文件，可以用于测试工具功能。`,
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		outputPath := args[0]
		// 输出文件路径信息
		fmt.Printf("生成测试文件到: %s\n", outputPath)
		
		// 获取行数
		rowCount, _ := cmd.Flags().GetInt("rows")
		if rowCount <= 0 {
			rowCount = 100 // 默认生成100行
		}
		
		// 获取列定义
		schemaStr, _ := cmd.Flags().GetString("schema")
		if schemaStr == "" {
			// 使用默认schema
			schemaStr = `{"id":"INT64","name":"UTF8","age":"INT32","weight":"FLOAT","active":"BOOLEAN"}`
		}
		
		// 解析schema
		var schema map[string]string
		if err := json.Unmarshal([]byte(schemaStr), &schema); err != nil {
			er(fmt.Sprintf("无法解析schema: %v", err))
			return
		}
		
		// 创建parquet文件
		fw, err := local.NewLocalFileWriter(outputPath)
		if err != nil {
			er(fmt.Sprintf("无法创建输出文件: %v", err))
			return
		}
		defer fw.Close()

		// 创建示例数据，用于推断schema
		firstRow := make(map[string]interface{})
		for field, typ := range schema {
			// 根据类型设置初始值
			switch typ {
			case "INT64":
				firstRow[field] = int64(0)
			case "INT32":
				firstRow[field] = int32(0)
			case "FLOAT":
				firstRow[field] = float32(0)
			case "DOUBLE":
				firstRow[field] = float64(0)
			case "BOOLEAN":
				firstRow[field] = false
			case "UTF8", "STRING":
				firstRow[field] = ""
			default:
				firstRow[field] = "" // 默认为空字符串
			}
		}

		// 构建schema字符串
		parquetSchema := "{"
		parquetSchema += "\"Tag\":\"name=parquet-go-root\","
		parquetSchema += "\"Fields\":["
		
		firstItem := true
		for field, typ := range schema {
			if !firstItem {
				parquetSchema += ","
			}
			firstItem = false
			
			// 映射数据类型到Parquet类型
			parquetType := "BYTE_ARRAY"
			convertedType := "UTF8"
			
			switch typ {
			case "INT64":
				parquetType = "INT64"
				convertedType = ""
			case "INT32":
				parquetType = "INT32"
				convertedType = ""
			case "FLOAT":
				parquetType = "FLOAT"
				convertedType = ""
			case "DOUBLE":
				parquetType = "DOUBLE"
				convertedType = ""
			case "BOOLEAN":
				parquetType = "BOOLEAN"
				convertedType = ""
			case "UTF8", "STRING":
				parquetType = "BYTE_ARRAY"
				convertedType = "UTF8"
			}
			
			fieldSchema := fmt.Sprintf("{\"Tag\":\"name=%s, type=%s", field, parquetType)
			if convertedType != "" {
				fieldSchema += fmt.Sprintf(", convertedtype=%s", convertedType)
			}
			fieldSchema += "\"}"
			
			parquetSchema += fieldSchema
		}
		
		parquetSchema += "]}"

		// 创建写入器
		pw, err := writer.NewJSONWriter(parquetSchema, fw, 4)
		if err != nil {
			er(fmt.Sprintf("无法创建Parquet写入器: %v", err))
			return
		}
		
		// 设置写入选项
		pw.RowGroupSize = 128 * 1024 * 1024 // 128MB
		pw.PageSize = 8 * 1024             // 8KB

		// 写入测试数据
		for i := 0; i < rowCount; i++ {
			data := make(map[string]interface{})
			for field, typ := range schema {
				// 根据类型生成合适的数据
				switch typ {
				case "INT64":
					data[field] = int64(i)
				case "INT32":
					data[field] = int32(20 + i%10)
				case "FLOAT":
					data[field] = float32(50.0 + float32(i)*0.1)
				case "DOUBLE":
					data[field] = float64(50.0 + float64(i)*0.1)
				case "BOOLEAN":
					data[field] = (i%2 == 0)
				case "UTF8", "STRING":
					data[field] = fmt.Sprintf("用户%d", i)
				default:
					data[field] = fmt.Sprintf("值%d", i)
				}
			}

			// 转换为JSON字符串
			jsonData, err := json.Marshal(data)
			if err != nil {
				er(fmt.Sprintf("序列化行数据失败: %v", err))
				return
			}
			
			if err := pw.Write(string(jsonData)); err != nil {
				er(fmt.Sprintf("写入数据失败: %v", err))
				return
			}
		}

		if err := pw.WriteStop(); err != nil {
			er(fmt.Sprintf("完成写入失败: %v", err))
			return
		}

		// 输出schema信息
		fmt.Println("生成的schema:")
		json.NewEncoder(os.Stdout).Encode(schema)
		fmt.Printf("成功生成测试文件: %s，包含 %d 行数据\n", outputPath, rowCount)
	},
}

func init() {
	rootCmd.AddCommand(generateCmd)
	generateCmd.Flags().IntP("rows", "r", 100, "要生成的数据行数")
	generateCmd.Flags().StringP("schema", "s", "", `数据的schema定义，JSON格式，如：{"id":"INT64","name":"UTF8","age":"INT32"}。支持的类型有INT64、INT32、FLOAT、DOUBLE、BOOLEAN、UTF8/STRING`)
} 