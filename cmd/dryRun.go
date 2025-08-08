/*
Copyright © 2025 Matthew Thornton
*/
package cmd

import (
	"dibbity/core"
	//"fmt"
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
	var err error

	isVerbose, err := cmd.Flags().GetBool("verbose")
	if err != nil {
		log.Fatalf("Error getting verbose flag: %v", err)
	}

	dbtDir, err := core.GetFolder(isVerbose)
	if err != nil {
		log.Fatalf("Error getting dbt directory: %v", err)
	}

	isCompile, err := cmd.Flags().GetBool("compile")
	if err != nil {
		log.Fatalf("Error getting compile flag: %v", err)
	}

	selectedModels, err := cmd.Flags().GetStringSlice("select")
	if err != nil {
		log.Fatalf("Error getting selected models: %v", err)
	}
	selectedModels = append(selectedModels, args...)

	core.LogVerbose(isVerbose, "Selected models: %v", selectedModels)

	// TODO: add defer flags
	if isCompile {
		err = core.CompileModel(selectedModels, dbtDir, isVerbose)
		if err != nil {
			log.Fatalf("Error compiling model: %v", err)
		}
	}

	modelMap = make(map[string]string)
	for _, model := range selectedModels {
		fp, err := core.FindFilepath(model, dbtDir, "target", isVerbose)
		if err != nil {
			log.Fatalf("Error finding model %s: %v", model, err)
		}
		modelMap[model] = fp
	}

	core.LogVerbose(isVerbose, "%v", modelMap)

	q, err := core.LoadSQL(modelMap[selectedModels[0]], isVerbose)
	if err != nil {
		log.Fatalf("Error loading SQL: %v", err)
	}

	//var bqout string
	_, err = core.BqDryRun(q, isVerbose)
	if err != nil {
		log.Fatalf("Error in BQ dry run: %v", err)
	}

	core.LogVerbose(isVerbose, "Done")
}

func init() {
	rootCmd.AddCommand(dryRunCmd)

	dryRunCmd.Flags().StringSliceP("select", "s", []string{}, "Select models to run")
	if err := dryRunCmd.MarkFlagRequired("select"); err != nil {
		log.Fatalf("Error: could not mark --select flag as required: %v", err)
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
