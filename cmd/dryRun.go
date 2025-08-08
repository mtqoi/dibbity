/*
Copyright © 2025 Matthew Thornton
*/
package cmd

import (
	"dibbity/core"
	"fmt"
	"github.com/spf13/cobra"
	"log"
)

var dryRunCmd = &cobra.Command{
	Use:   "dryRun",
	Short: "Dry run selected models",
	Long:  "Dry run selected models",
	Run:   dryRunRun,
}

var modelMap map[string]string

func dryRunRun(cmd *cobra.Command, args []string) {

	isVerbose, _ := cmd.Flags().GetBool("verbose")
	dbtDir := core.GetFolder(isVerbose)
	isCompile, _ := cmd.Flags().GetBool("compile")

	selectedModels, _ := cmd.Flags().GetStringSlice("select")
	selectedModels = append(selectedModels, args...)

	core.LogVerbose(isVerbose, "Selected models: %v", selectedModels)

	// TODO: add defer flags
	if isCompile {
		err := core.CompileModel(selectedModels, dbtDir, isVerbose)
		if err != nil {
			log.Fatalf("Could not compile model: %v", err)
		}
	}

	modelMap = make(map[string]string)
	for _, model := range selectedModels {
		fp, err := core.FindFilepath(model, dbtDir, "target", isVerbose)
		if err != nil {
			log.Fatalf("Could not find model: %v", err)
		}
		modelMap[model] = fp
	}

	core.LogVerbose(isVerbose, "%v", modelMap)

	q, err := core.LoadSQL(selectedModels[0], dbtDir, isVerbose)
	if err != nil {
		log.Fatalf("Could not load SQL: %v", err)
	}

	var bqout string
	bqout, err = core.BqDryRun(q, isVerbose)
	if err != nil {
		log.Fatalf("BQ dry run failed: %v", err)
	}

	fmt.Println(bqout)

	core.LogVerbose(isVerbose, "Done")
}

func init() {
	rootCmd.AddCommand(dryRunCmd)

	dryRunCmd.Flags().StringSliceP("select", "s", []string{}, "Select models to run")
	if err := dryRunCmd.MarkFlagRequired("select"); err != nil {
		log.Fatalf("Could not mark --select flag as required: %v", err)
	}

	dryRunCmd.Flags().BoolP("compile", "c", false, "Compile new model")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// dryRunCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// dryRunCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
