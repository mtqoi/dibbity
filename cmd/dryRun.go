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

type Model struct {
	Name       string
	Path       string
	SQL        string
	CostBytes  int
	BQResponse string
}

func dryRunRun(cmd *cobra.Command, args []string) {
	var err error

	// TODO: refactor to bind to variable directly
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
	// compile the models if required
	if isCompile {
		err = core.CompileModel(selectedModels, dbtDir, isVerbose)
		if err != nil {
			log.Fatalf("Error compiling model: %v", err)
		}
	}

	// initialise empty list of models
	var models []Model

	// populate the fields
	for _, modelName := range selectedModels {
		fp, err := core.FindFilepath(modelName, dbtDir, "target", isVerbose)
		if err != nil {
			log.Fatalf("Error finding model %s: %v", modelName, err)
		}
		sql, err := core.LoadSQL(fp, isVerbose)
		if err != nil {
			log.Fatalf("Error loading SQL for model %s: %v", modelName, err)
		}
		models = append(models, Model{Name: modelName, Path: fp, SQL: sql})
	}

	core.LogVerbose(isVerbose, "Loaded %d models", len(models))

	if len(models) == 0 {
		log.Fatalln("No models selected or found.")
	}

	// TODO: add in control logic to do the dryrun + get the info back

	models[0].BQResponse, err = core.BqDryRun(models[0].SQL, isVerbose)
	if err != nil {
		log.Fatalf("Error running dry run: %v", err)
	}

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
