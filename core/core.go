package core

import (
	"fmt"
	"github.com/spf13/viper"
	"log"
	"os"
	"os/exec"
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
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
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
