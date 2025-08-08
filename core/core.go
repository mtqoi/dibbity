package core

import (
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// GetFolder retrieves the directory path specified by the "dbt-dir" configuration key, resolving "~" to the user's home directory.
func GetFolder(b bool) string {
	dbtDir := viper.GetString("dbt-dir")

	if b {
		fmt.Printf("Using dbt folder: %s\n", dbtDir)
	}

	if strings.HasPrefix(dbtDir, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			log.Fatalf("Failed to get user home directory: %v", err)
		}
		dbtDir = strings.Replace(dbtDir, "~", home, 1)
	}
	return dbtDir
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
	if b {
		fmt.Printf("Running: %s\n", a)
	}
	c := exec.Command("zsh", "-c", a)

	c.Dir = dir

	// TODO: ensure these only print if we have the verbose flag set
	if b {
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
	}
	err := c.Run()
	if err != nil {
		log.Fatalf("Could not activate Poetry in %s: %v", dir, err)
	}
	return a, err
}

func ListModels(m []string, dir string) error {
	for _, model := range m {
		fmt.Println(model)
	}

	return nil
}

func CompileModel(modelName string, dir string, b bool) error {

	a := fmt.Sprintf("dbt compile --select %s", modelName)

	_, err := PoetryRun(a, dir, b)
	if err != nil {
		log.Fatalf("Could not compile model %s: %v", modelName, err)
	}
	return nil
}

var errFound = errors.New("found")

// FindFilepath finds the path to the model sql in dir/subdir
func FindFilepath(modelName string, dir string, subDir string, b bool) (string, error) {
	var filePath string

	dir = filepath.Join(dir, subDir)
	if b {
		fmt.Printf("Looking for model %s in %s\n", modelName, dir)
	}
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

	if b {
		fmt.Printf("Found model %s at %s\n", modelName, filePath)
	}

	return filePath, nil
}
