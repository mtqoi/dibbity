/*
Copyright © 2025 Matthew Thornton
*/
package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"os"
	"os/exec"
	"strings"
)

var dryRunCmd = &cobra.Command{
	Use:   "dryRun",
	Short: "Dry run selected models",
	Long:  "Dry run selected models",
	Run:   dryRunRun,
}

func dryRunRun(cmd *cobra.Command, args []string) {

	dbtDir := getFolder()
	fmt.Printf("Using dbt folder: %s\n", dbtDir) // TODO: remove

	//var err error

	//a, err := poetryRun("dbt --version", dbtDir)
	//if err != nil {
	//	log.Fatalf("Could not run %s in %s: %v", a, dbtDir, err)
	//}

	selectedModels, _ := cmd.Flags().GetStringSlice("select")
	selectedModels = append(selectedModels, args...)

	fmt.Printf("Selected models: %v\n", selectedModels)

	err := listModels(selectedModels, dbtDir)
	if err != nil {
		log.Fatalf("Could not list models: %v", err)
	}
}

// getFolder retrieves the directory path specified by the "dbt-dir" configuration key, resolving "~" to the user's home directory.
func getFolder() string {
	dbtDir := viper.GetString("dbt-dir")

	if strings.HasPrefix(dbtDir, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			log.Fatalf("Failed to get user home directory: %v", err)
		}
		dbtDir = strings.Replace(dbtDir, "~", home, 1)
	}
	return dbtDir
}

// listDir runs `ls -l` in directory path specified by the "dbt-dir" configuration key
func listDir(dir string) error {
	c := exec.Command("ls", "-l")

	c.Dir = dir // change the directory
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	err := c.Run()

	return err
}

// poetryRun can run arbitrary commands in the directory path specified by the "dbt-dir" configuration key
func poetryRun(args string, dir string) (string, error) {
	a := fmt.Sprintf("poetry run %s", args)
	c := exec.Command("zsh", "-c", a)

	c.Dir = dir
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	err := c.Run()
	if err != nil {
		log.Fatalf("Could not activate Poetry in %s: %v", dir, err)
	}
	return a, err
}

func listModels(m []string, dir string) error {
	for _, model := range m {
		fmt.Println(model)
	}

	return nil
}

func init() {
	rootCmd.AddCommand(dryRunCmd)

	dryRunCmd.Flags().StringSliceP("select", "s", []string{}, "Select models to run")
	if err := dryRunCmd.MarkFlagRequired("select"); err != nil {
		log.Fatalf("Could not mark --select flag as required: %v", err)
	}

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// dryRunCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// dryRunCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
