package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// TODO: consider adding a Program type which will contain the flags to pass into dbt

type DbtOptions struct {
	Command string   // the command to actually run
	Select  []string // the models we want to run
	Empty   bool
	Defer   bool
	Compile bool // do we want to first compile the models?
}

func (opts *DbtOptions) BuildArgs() []string {
	args := []string{opts.Command}
	if len(opts.Select) > 0 {
		args = append(args, "--select")
		args = append(args, opts.Select...)
	}

	if opts.Defer {
		args = append(args, []string{"--defer", "--state", "target_prod", "--favor-state"}...)
	}

	if opts.Empty {
		args = append(args, "--empty")
	}

	return args
}

type BqRunner struct {
	Query          string
	Out            string
	BytesProcessed int64
	// TODO: also check the docs for other things to add
	// TODO: also handle case where we can't run the query
}

type BqDryRunResponse struct {
	TotalBytesProcessed int64 `json:"-"` // Not directly mapped from JSON
}

// custom json unmarshall;er
func (r *BqDryRunResponse) UnmarshalJSON(data []byte) error {
	var raw struct {
		Statistics struct {
			Query struct {
				TotalBytesProcessed string `json:"totalBytesProcessed"`
			} `json:"query"`
		} `json:"statistics"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Convert string to int64
	if raw.Statistics.Query.TotalBytesProcessed != "" {
		var err error
		r.TotalBytesProcessed, err = strconv.ParseInt(raw.Statistics.Query.TotalBytesProcessed, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse TotalBytesProcessed: %w", err)
		}
	}

	return nil
}

func (bq *BqRunner) BqDryRun(b bool) (*BqRunner, error) {
	var out bytes.Buffer
	var stderr bytes.Buffer

	args := []string{"query", "--nouse_legacy_sql", "--dry_run", "--nouse_cache", "--format=json", bq.Query}
	LogVerbose(b, "Running: bq %s", strings.Join(args, " "))

	c := exec.Command("bq", args...)

	c.Stdout = &out
	c.Stderr = &stderr

	err := c.Run()
	if err != nil {
		return &BqRunner{}, fmt.Errorf("bq command failed with error: %w, stderr: %s", err, stderr.String())
	}

	bq.Out = out.String()
	var stats BqDryRunResponse
	if err := json.Unmarshal(out.Bytes(), &stats); err != nil {
		return &BqRunner{}, fmt.Errorf("failed to unmarshal bq dry run response: %w", err)
	}
	bq.BytesProcessed = stats.TotalBytesProcessed

	// TODO: need to add a check as to whether the code runs correctly

	return bq, nil
}

func LogVerbose(b bool, format string, a ...interface{}) {
	if !b {
		return
	}
	const pinkColor = "\033[95m"
	const resetColor = "\033[0m"

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	message := fmt.Sprintf(format, a...)
	fmt.Printf("%s%s | %s%s\n", pinkColor, timestamp, message, resetColor)
}

// GetFolder retrieves the directory path specified by the "dbt-dir" configuration key, resolving "~" to the user's home directory.
func GetFolder(b bool) (string, error) {
	dbtDir := viper.GetString("dbt-dir")

	LogVerbose(b, "Using dbt folder: %s", dbtDir)

	if strings.HasPrefix(dbtDir, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to get user home directory: %w", err)
		}
		dbtDir = strings.Replace(dbtDir, "~", home, 1)
	}
	return dbtDir, nil
}

// ListDir runs `ls -l` in directory path specified by the "dbt-dir" configuration key
func ListDir(dir string, b bool) error {
	c := exec.Command("ls", "-l")

	c.Dir = dir // change the directory
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	err := c.Run()

	return err
}

// PoetryRun can run arbitrary commands in the directory path specified by the "dbt-dir" configuration key
func PoetryRun(program string, args []string, dir string, b bool) error {
	cmdArgs := []string{"run", program}
	cmdArgs = append(cmdArgs, args...)

	LogVerbose(b, "Running: poetry %s", strings.Join(cmdArgs, " "))

	//c := exec.Command("zsh", "-c", a)
	c := exec.Command("poetry", cmdArgs...)

	c.Dir = dir

	// TODO: rethink how I want to capture the response here
	if b {
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
	}

	err := c.Run()
	if err != nil {
		return err
	}
	return nil
}

func ListModels(m []string, dir string, b bool) error {

	opts := DbtOptions{
		Select:  m,
		Command: "ls",
	}
	args := opts.BuildArgs()
	args = append(args, "--resource-type model")
	err := PoetryRun("dbt", args, dir, b)
	if err != nil {
		return err
	}

	return nil
}

// CompileModel compiles the dbt models set in DbtOptions.Select
func CompileModel(opts DbtOptions, dir string, b bool) error {

	opts.Command = "compile"
	args := opts.BuildArgs()

	err := PoetryRun("dbt", args, dir, b)
	if err != nil {
		return err
	}
	return nil
}

// LoadSQL grabs the sql
func LoadSQL(f string, b bool) (string, error) {

	LogVerbose(b, "Loading SQL from %s", f)

	var q []byte
	q, err := os.ReadFile(f)
	if err != nil {
		return "", err
	}

	return string(q), nil
}

var errFound = errors.New("found")

// FindFilepath finds the path to the model sql in dir/subdir
func FindFilepath(modelName string, dir string, subDir string, b bool) (string, error) {
	var filePath string

	dir = filepath.Join(dir, subDir)
	LogVerbose(b, "Looking for model %s in %s", modelName, dir)
	modelFilename := modelName + ".sql"

	walkErr := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && info.Name() == modelFilename {
			filePath = path
			return errFound // stop walking since we found our file
		}
		return nil
	})

	if walkErr != nil && !errors.Is(walkErr, errFound) {
		return "", fmt.Errorf("error walking the path %q: %w", dir, walkErr)
	}

	if filePath == "" {
		return "", fmt.Errorf("model '%s' not found", modelName)
	}

	LogVerbose(b, "Found model %s at %s", modelName, filePath)

	return filePath, nil
}
