/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"dibbity/core"
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"log"
	"os/exec"
	"runtime"
	"strings"

	"github.com/spf13/cobra"
)

// openCmd represents the open command
var openCmd = &cobra.Command{
	Use:   "open",
	Short: "Open selected models in BigQuery Studio",
	Long: `
 	Open selected models in BigQuery Studio
`,
	Run: openCmdRun,
}

type bqUrlBuilder struct {
	baseUrl     string
	projectID   string
	datasetName string
	tableName   string
}

var selectedModel string

func openCmdRun(cmd *cobra.Command, args []string) {
	// TODO: add some verbose printing
	isVerbose := viper.GetBool("verbose")
	dbtDir, err := core.GetFolder(isVerbose)

	if err != nil {
		log.Fatalf("Error getting dbt folder: %v", err)
	}

	fp, err := core.FindFilepath(selectedModel, dbtDir, "models", isVerbose)
	if err != nil {
		log.Fatalf("Error finding model %s: %v", selectedModel, err)
	}

	bqb, err := formatModelPath(fp)
	if err != nil {
		log.Fatalf("Error formatting model path: %v", err)
	}

	// TODO: set these as parameters in dibbity.yml
	bqb.baseUrl = "https://console.cloud.google.com/bigquery"
	bqb.projectID = "data-trustedwarehouse-p"
	url := getUrl(bqb)

	summary := fmt.Sprintf("%sOpening %s in browser%s", core.Magenta, bqb.tableName, core.Reset)
	fmt.Println(summary)

	if err := openBrowser(url); err != nil {
		log.Fatalf("Error opening browser: %v", err)
	}

}

func formatModelPath(modelPath string) (bqUrlBuilder, error) {

	modelsIndex := strings.Index(modelPath, "dags/templates/models/")
	if modelsIndex == -1 {
		return bqUrlBuilder{}, errors.New("could not find models index in model path")
	}

	relevantPath := modelPath[modelsIndex+len("dags/templates/models/"):]
	relevantPath = strings.TrimSuffix(relevantPath, ".sql")

	pathComponents := strings.Split(relevantPath, "/")

	if len(pathComponents) <= 2 {
		return bqUrlBuilder{}, errors.New("could not find project, dataset, and table in model path")
	}

	tableName := pathComponents[len(pathComponents)-1]
	datasetComponents := pathComponents[:len(pathComponents)-1]
	datasetName := strings.Join(datasetComponents, "_")

	bqb := bqUrlBuilder{
		datasetName: datasetName,
		tableName:   tableName,
	}

	return bqb, nil
}

func getUrl(bqb bqUrlBuilder) string {

	s := fmt.Sprintf("%s?p=%s&d=%s&t=%s&page=table", bqb.baseUrl, bqb.projectID, bqb.datasetName, bqb.tableName)

	return s
}

func openBrowser(url string) error {
	var err error

	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	default:
		err = fmt.Errorf("unsupported platform")
	}

	return err
}

func init() {
	rootCmd.AddCommand(openCmd)

	openCmd.Flags().StringVarP(&selectedModel, "select", "s", "", "Select model to open")
	if err := dryRunCmd.MarkFlagRequired("select"); err != nil {
		log.Fatalf("Error: could not mark --select flag as required: %v", err)
	}

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// openCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// openCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
