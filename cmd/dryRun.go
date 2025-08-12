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
	"strings"
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

// TODO: I am sure I should refactor this and split out the functionality
func dryRunRun(cmd *cobra.Command, args []string) {
	var err error

	dbtOpts := core.DbtOptions{
		Select:  selectedModels,
		Empty:   shouldEmptyBuild,
		Defer:   shouldDefer,
		Compile: shouldCompile,
	}

	isVerbose := viper.GetBool("verbose")
	dbtDir, err := core.GetFolder(isVerbose)
	if err != nil {
		log.Fatalf("Error getting dbt folder: %v", err)
	}

	selectedModels = append(selectedModels, args...)
	// Print fancy header
	fmt.Println()
	// TODO: ensure that the length of models is using the expanded number
	core.PrintBox(fmt.Sprintf("BigQuery Dry Run: %d models", len(selectedModels)), fmt.Sprintf("Models: %s", strings.Join(selectedModels, ", ")), core.BoxRounded, core.BrightCyan)
	fmt.Println()
	selectedModels, err = core.ListModels(selectedModels, dbtDir, isVerbose)
	if err != nil {
		log.Fatalf("error running dbt ls: %v", err)
	}

	core.LogVerbose(isVerbose, "Selected models: %v", selectedModels)

	// TODO: add some pretty printing for when the model is compiling
	if dbtOpts.Compile {
		core.ColorPrint(core.Bold+core.BrightBlue, "⚙️  ")
		core.ColorPrintln(core.Bold+core.BrightBlue, "Compiling DBT models...")

		err = core.CompileModel(dbtOpts, dbtDir, isVerbose)
		if err != nil {
			// Show error in a styled box if compilation fails
			core.ColorPrintln(core.Bold+core.BrightRed, "❌ Compilation failed!")
			core.PrintBox("Compilation Error", fmt.Sprintf("%v", err), core.BoxRounded, core.Red)
			log.Fatalf("Error compiling models: %v", err)
		} else {
			// Show success message
			core.ColorPrint(core.Bold+core.Green, "✓ ")
			core.ColorPrintln(core.Bold+core.Green, "Compilation completed successfully!")
			fmt.Println() // Add a blank line for spacing
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

	for i := range models {
		// Create a fancy model header
		modelHeader := fmt.Sprintf("Model: %s", models[i].Name)
		core.ColorPrintln(core.Bold+core.BrightBlue, modelHeader)
		core.ColorPrintln(core.Dim+core.BrightBlue, strings.Repeat("─", len(modelHeader)))

		models[i].BQRunner = core.BqRunner{
			Query: models[i].SQL,
			Ok:    true,
		}

		_, err := models[i].BQRunner.BqDryRun(isVerbose)
		if err != nil {
			log.Fatalf("Error running dry run: %v", err)
		}

		models[i].CostBytes = int(models[i].BQRunner.BytesProcessed)

		if !models[i].BQRunner.Ok {
			core.ColorPrint(core.Bold+core.Red, "✗ ")
			core.ColorPrintln(core.Bold+core.Red, "Failed")
			core.PrintBox("Error", models[i].BQRunner.RespError, core.BoxRounded, core.Red)
		} else {
			core.ColorPrint(core.Bold+core.Green, "✓ ")
			core.ColorPrint(core.Bold, "Success - Data to process: ")
			fmt.Println(FormatCost(models[i].CostBytes))
		}
		fmt.Println() // Add spacing between models
	}

	// Calculate and print summary
	var totalCost int64
	var successCount, failCount int

	for _, model := range models {
		totalCost += int64(model.CostBytes)
		if model.BQRunner.Ok {
			successCount++
		} else {
			failCount++
		}
	}

	// Print summary in a fancy box
	summary := fmt.Sprintf(
		"Models Processed: %d\n"+
			"Successful: %s%d%s\n"+
			"Failed: %s%d%s\n"+
			"Total Data to Process: %s",
		len(models),
		core.Green, successCount, core.Reset,
		core.Red, failCount, core.Reset,
		core.FormatBytes(totalCost),
	)

	core.PrintBox("Dry Run Summary", summary, core.BoxDouble, core.BrightMagenta)
}

// FormatCost calculates the total cost in bytes of all models
// and returns a formatted string with appropriate units (B, MB, GB, TB)
func FormatCost(bytes int) string {
	return core.FormatBytes(int64(bytes))
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
