package core

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// TODO: consider adding a Program type which will contain the flags to pass into dbt

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
func PoetryRun(args string, dir string, b bool) (string, error) {
	a := fmt.Sprintf("poetry run %s", args)

	LogVerbose(b, "Running: %s", a)

	c := exec.Command("zsh", "-c", a)

	c.Dir = dir

	if b {
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
	}

	err := c.Run()
	if err != nil {
		return "", fmt.Errorf("could not activate Poetry in %s: %w", dir, err)
	}
	return a, nil
}

func ListModels(m []string, dir string, b bool) error {

	a := fmt.Sprintf("dbt ls --select %s %s", strings.Join(m, " "), "--resource-type model")

	_, err := PoetryRun(a, dir, b)
	if err != nil {
		return fmt.Errorf("could not list models: %w", err)
	}

	return nil
}

func CompileModel(m []string, dir string, b bool) error {

	a := fmt.Sprintf("dbt compile --select %s", strings.Join(m, " "))

	_, err := PoetryRun(a, dir, b)
	if err != nil {
		return fmt.Errorf("could not compile models %s: %w", strings.Join(m, " "), err)
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

// Q: is there a bq runner library already for Go?
func BqDryRun(q string, b bool) (string, error) {

	var out bytes.Buffer
	var stderr bytes.Buffer
	c := exec.Command("bq", "query", "--nouse_legacy_sql", "--dry_run", "--nouse_cache", q)

	c.Stdout = &out
	c.Stderr = &stderr

	err := c.Run()
	if err != nil {
		return "", err
	}

	if b {
		fmt.Print(out.String())
	}

	return out.String(), nil
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
