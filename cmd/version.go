package cmd

import (
	"fmt"
	"runtime"
	"runtime/debug"

	"github.com/spf13/cobra"
)

// versionCmd represents the version command
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Long:  `Print version information of pq-tools and its dependencies.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Print pq-tools version
		fmt.Println("pq-tools version:", getVersion())

		// Print Go version
		fmt.Println("Go version:", runtime.Version())

		// Print dependency versions
		fmt.Println("\nDependencies:")
		fmt.Println("- parquet-go:", getDependencyVersion("github.com/parquet-go/parquet-go"))
		fmt.Println("- cobra:", getDependencyVersion("github.com/spf13/cobra"))
	},
}

func getVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	return bi.Main.Version
}

func getDependencyVersion(path string) string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range bi.Deps {
		if dep.Path == path {
			return dep.Version
		}
	}
	return "unknown"
}

func init() {
	rootCmd.AddCommand(versionCmd)
} 