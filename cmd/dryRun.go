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

func dryRunRun(cmd *cobra.Command, args []string) {

	isVerbose, _ := cmd.Flags().GetBool("verbose")
	dbtDir := core.GetFolder(isVerbose)
	isCompile, _ := cmd.Flags().GetBool("compile")

	//var err error

	//a, err := poetryRun("dbt --version", dbtDir)
	//if err != nil {
	//	log.Fatalf("Could not run %s in %s: %v", a, dbtDir, err)
	//}

	selectedModels, _ := cmd.Flags().GetStringSlice("select")
	selectedModels = append(selectedModels, args...)

	if isVerbose {
		fmt.Printf("Selected models: %v\n", selectedModels)
	}

	//err := core.ListModels(selectedModels, dbtDir)
	//if err != nil {
	//	log.Fatalf("Could not list models: %v", err)
	//}

	if isCompile {
		err := core.CompileModel(selectedModels[0], dbtDir, isVerbose)
		if err != nil {
			log.Fatalf("Could not compile model: %v", err)
		}
	}

	// TODO: ensure this can work for multiple models
	_, err := core.FindFilepath(selectedModels[0], dbtDir, "target", isVerbose)
	if err != nil {
		log.Fatalf("Could not find model: %v", err)
	}

	fmt.Println("Done")
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
