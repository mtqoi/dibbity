/*
Copyright © 2025 Matthew Thornton
*/
package cmd

import (
	"dibbity/core"
	"github.com/spf13/viper"

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

type Model struct {
	Name      string
	Path      string
	SQL       string
	CostBytes int
	BQRunner  core.BqRunner
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
	dbtDir, err := core.GetFolder(isVerbose)
	if err != nil {
		log.Fatalf("Error getting dbt folder: %v", err)
	}

	// TODO: I need use `dbt ls` to expand the models given
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

	for i := range models {

		fmt.Println("Running model: ", models[i].Name) // TODO: make pretty

		models[i].BQRunner = core.BqRunner{
			Query: models[i].SQL,
			Ok:    true,
		}

		_, err := models[i].BQRunner.BqDryRun(isVerbose)
		if err != nil {
			log.Fatalf("Error running dry run: %v", err) // TODO: decide whether I want to be able to recover from this
		}

		//model.BQRunner = *bqr
		models[i].CostBytes = int(models[i].BQRunner.BytesProcessed)

		// TODO: make pretty
		if !models[i].BQRunner.Ok {
			fmt.Printf("Dry run failed for model %s: %s\n", models[i].Name, models[i].BQRunner.RespError)
		} else {
			fmt.Printf("Dry run successful for model %s, total cost: %s\n",
				models[i].Name, FormatCost(models[i].CostBytes))
		}
	}

	var totalCost int
	var formattedTotalCost string

	for _, model := range models {
		totalCost += model.CostBytes
	}

	formattedTotalCost = FormatCost(totalCost)

	fmt.Println("\n==== Dry Run Summary ====")
	fmt.Printf("total models procesed: %d\n", len(models))
	fmt.Printf("Total data processed: %s\n", formattedTotalCost)
}

// FormatCost calculates the total cost in bytes of all models
// and returns a formatted string with appropriate units (B, MB, GB, TB)
func FormatCost(m int) string {

	// format with the appropriate unit
	var formattedCost string
	switch {
	case m < 1024:
		formattedCost = fmt.Sprintf("%d B", m)
	case m < 1024*1024:
		formattedCost = fmt.Sprintf("%.2f KB", float64(m)/1024)
	case m < 1024*1024*1024:
		formattedCost = fmt.Sprintf("%.2f MB", float64(m)/(1024*1024))
	case m < 1024*1024*1024*1024:
		formattedCost = fmt.Sprintf("%.2f GB", float64(m)/(1024*1024*1024))
	case m < 1024*1024*1024*1024*1024:
		formattedCost = fmt.Sprintf("%.2f TB", float64(m)/(1024*1024*1024*1024))
	}

	return formattedCost
}

func init() {
	rootCmd.AddCommand(dryRunCmd)

	dryRunCmd.Flags().StringSliceVarP(&selectedModels, "select", "s", []string{}, "Select models to run")
	if err := dryRunCmd.MarkFlagRequired("select"); err != nil {
		log.Fatalf("Error: could not mark --select flag as required: %v", err)
	}

	dryRunCmd.Flags().BoolVarP(&shouldCompile, "compile", "c", false, "Compile new model")
	dryRunCmd.Flags().BoolVarP(&shouldDefer, "defer", "d", true, "Use deferred build")
	dryRunCmd.Flags().BoolVarP(&shouldEmptyBuild, "empty", "e", false, "Use empty build")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// dryRunCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// dryRunCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
