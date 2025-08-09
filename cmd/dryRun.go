/*
Copyright © 2025 Matthew Thornton
*/
package cmd

import (
	"dibbity/core"
	"github.com/spf13/viper"

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

var (
	selectedModels   []string
	shouldCompile    bool
	shouldDefer      bool
	shouldEmptyBuild bool
)

func dryRunRun(cmd *cobra.Command, args []string) {
	var err error

	isVerbose := viper.GetBool("verbose")
	dbtDir := viper.GetString("dbt-dir")

	selectedModels = append(selectedModels, args...)

	core.LogVerbose(isVerbose, "Selected models: %v", selectedModels)

	dbtOpts := core.DbtOptions{
		Select:  selectedModels,
		Empty:   shouldEmptyBuild,
		Defer:   shouldDefer,
		Compile: shouldCompile,
	}

	if dbtOpts.Compile {
		err = core.CompileModel(dbtOpts, dbtDir, isVerbose)
		if err != nil {
			log.Fatalf("Error compiling models: %v", err)
		}
	}

	var models []Model

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

	dryRunCmd.Flags().StringSliceVarP(&selectedModels, "select", "s", []string{}, "Select models to run")
	if err := dryRunCmd.MarkFlagRequired("select"); err != nil {
		log.Fatalf("Error: could not mark --select flag as required: %v", err)
	}

	dryRunCmd.Flags().BoolVarP(&shouldCompile, "compile", "c", false, "Compile new model")
	dryRunCmd.Flags().BoolVarP(&shouldDefer, "defer", "d", false, "Use deferred build")
	dryRunCmd.Flags().BoolVarP(&shouldEmptyBuild, "empty", "e", false, "Use empty build")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// dryRunCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// dryRunCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
