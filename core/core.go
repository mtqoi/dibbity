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
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Color constants for terminal output
const (
	// Basic colors
	Reset     = "\033[0m"
	Bold      = "\033[1m"
	Dim       = "\033[2m"
	Italic    = "\033[3m"
	Underline = "\033[4m"

	// Foreground colors
	Black   = "\033[30m"
	Red     = "\033[31m"
	Green   = "\033[32m"
	Yellow  = "\033[33m"
	Blue    = "\033[34m"
	Magenta = "\033[35m"
	Cyan    = "\033[36m"
	White   = "\033[37m"

	// Bright foreground colors
	BrightBlack   = "\033[90m"
	BrightRed     = "\033[91m"
	BrightGreen   = "\033[92m"
	BrightYellow  = "\033[93m"
	BrightBlue    = "\033[94m"
	BrightMagenta = "\033[95m"
	BrightCyan    = "\033[96m"
	BrightWhite   = "\033[97m"

	// Background colors
	BgBlack   = "\033[40m"
	BgRed     = "\033[41m"
	BgGreen   = "\033[42m"
	BgYellow  = "\033[43m"
	BgBlue    = "\033[44m"
	BgMagenta = "\033[45m"
	BgCyan    = "\033[46m"
	BgWhite   = "\033[47m"
)

// TODO: move all this to its own package

// ColorPrint prints text with specified color(s) and resets color after
func ColorPrint(colors string, text string) {
	fmt.Print(colors + text + Reset)
}

// ColorPrintf prints formatted text with specified color(s) and resets color after
func ColorPrintf(colors string, format string, a ...interface{}) {
	fmt.Print(colors + fmt.Sprintf(format, a...) + Reset)
}

// ColorPrintln prints text with specified color(s), resets color after, and adds a newline
func ColorPrintln(colors string, text string) {
	fmt.Println(colors + text + Reset)
}

// Box types for output styling
const (
	BoxSingle  = 0 // ┌─┐│└┘
	BoxDouble  = 1 // ╔═╗║╚╝
	BoxRounded = 2 // ╭─╮│╰╯
	BoxBold    = 3 // ┏━┓┃┗┛
	BoxSimple  = 4 // +--+|+--+
)

// BoxChars holds the characters for drawing boxes
var BoxChars = [][]string{
	{"\u250C", "\u2500", "\u2510", "\u2502", "\u2514", "\u2518"}, // Single
	{"\u2554", "\u2550", "\u2557", "\u2551", "\u255A", "\u255D"}, // Double
	{"\u256D", "\u2500", "\u256E", "\u2502", "\u2570", "\u256F"}, // Rounded
	{"\u250F", "\u2501", "\u2513", "\u2503", "\u2517", "\u251B"}, // Bold
	{"+", "-", "+", "|", "+", "+"},                               // Simple
}

// FormatBytes formats bytes into human-readable format with appropriate units
func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	// Define appropriate color based on size
	var color string
	switch exp {
	case 0:
		color = Green // KB
	case 1:
		color = Yellow // MB
	case 2:
		color = Magenta // GB
	default:
		color = Red // TB or larger
	}

	return fmt.Sprintf("%s%.2f %ciB%s", color, float64(bytes)/float64(div), "KMGTPE"[exp], Reset)
}

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
	Ok             bool
	RespError      string
	// TODO: also check the docs for other things to add
}

type BqDryRunResponse struct {
	TotalBytesProcessed int64 `json:"-"` // Not directly mapped from JSON
}

// UnmarshalJSON is a custom json unmarshaller
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

	args := []string{"query", "--nouse_legacy_sql", "--dry_run", "--nouse_cache", "--format=json"}

	// Print a fancy command execution message
	if b {
		cmdStr := fmt.Sprintf("bq %s", strings.Join(args, " "))
		ColorPrint(Bold+Green, "→ ")
		ColorPrint(Bold, "Executing: ")
		ColorPrintln(BrightYellow, cmdStr)
		ColorPrintln(Dim, "  Query being passed via stdin...")
	}
	c := exec.Command("bq", args...)

	c.Stdin = strings.NewReader(bq.Query) // piping in with stdin to ensure that queries beginning with `--` comment are interpreted as single arguments, not as an extra flag
	c.Stdout = &out
	c.Stderr = &stderr

	if b {
		ColorPrint(Blue, "⧗ ")
		ColorPrintln(Blue, "Running query analysis...")
	}

	err := c.Run()
	if err != nil {
		bq.Ok = false
		bq.RespError = out.String()

		if b {
			ColorPrintln(Bold+BgRed+White, " ERROR ")
			PrintBox("Query Analysis Failed", bq.RespError, BoxRounded, Red)
		}

		return bq, nil // return without error so the caller can check bq.Ok
	}

	bq.Out = out.String()
	bq.Ok = true

	var stats BqDryRunResponse
	if err := json.Unmarshal(out.Bytes(), &stats); err != nil {

		if b {
			ColorPrintln(Bold+BgRed+White, " ERROR ")
			PrintBox("Failed to parse response", err.Error(), BoxRounded, Red)
		}
		return &BqRunner{}, fmt.Errorf("failed to unmarshal bq dry run response: %w", err)
	}
	bq.BytesProcessed = stats.TotalBytesProcessed

	if b && bq.Ok {
		ColorPrint(Bold+Green, "✓ ")
		ColorPrintln(Bold+Green, "Analysis completed successfully!")

		// Show bytes processed with color coding by size
		ColorPrint(Bold, "Data to be processed: ")
		fmt.Println(FormatBytes(bq.BytesProcessed))
	}
	return bq, nil
}

func LogVerbose(b bool, format string, a ...interface{}) {
	if !b {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	message := fmt.Sprintf(format, a...)

	// Format: [TIME] MESSAGE
	ColorPrint(Dim+BrightBlack, "[")
	ColorPrint(BrightCyan, timestamp)
	ColorPrint(Dim+BrightBlack, "] ")
	fmt.Println(message)
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
func PoetryRun(program string, args []string, dir string, b bool) (string, error) {
	cmdArgs := []string{"run", program}
	cmdArgs = append(cmdArgs, args...)

	LogVerbose(b, "Running: poetry %s", strings.Join(cmdArgs, " "))

	//c := exec.Command("zsh", "-c", a)
	c := exec.Command("poetry", cmdArgs...)

	c.Dir = dir

	var outBuf = bytes.Buffer{}
	var errBuf = bytes.Buffer{}

	// TODO: rethink how I want to capture the response here
	if b {
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
	}
	c.Stdout = &outBuf
	c.Stderr = &errBuf

	err := c.Run()
	if err != nil {
		return errBuf.String(), err
	}
	return outBuf.String(), nil
}

func unmarshalNames(s string) ([]string, error) {
	// Split the input string by newlines
	lines := strings.Split(strings.TrimSpace(s), "\n")

	names := make([]string, 0, len(lines))

	// Process each line as a separate JSON object
	for _, line := range lines {
		// Skip empty lines
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}

		// Define a struct for the JSON object
		var item struct {
			Name string `json:"name"`
		}

		// Unmarshal the JSON object
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
		}

		// Add the name to the slice
		names = append(names, item.Name)
	}

	return names, nil
}

func ListModels(m []string, dir string, b bool) ([]string, error) {

	opts := DbtOptions{
		Command: "ls",
		Select:  m,
	}

	args := opts.BuildArgs()

	args = append(args, "--resource-type", "model", "--output", "json", "--output-keys", "name", "--quiet")
	s, err := PoetryRun("dbt", args, dir, b)

	if err != nil {
		return nil, err
	}

	names, err := unmarshalNames(s)
	if err != nil {
		return nil, err
	}
	return names, nil
}

// CompileModel compiles the dbt models set in DbtOptions.Select
func CompileModel(opts DbtOptions, dir string, b bool) error {

	opts.Command = "compile"
	args := opts.BuildArgs()

	_, err := PoetryRun("dbt", args, dir, b)
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

// StripANSI removes ANSI escape sequences from a string
func StripANSI(str string) string {
	// Match ANSI escape codes: \033[...m
	ansiRegex := regexp.MustCompile("\x1b\\[[0-9;]*m")
	return ansiRegex.ReplaceAllString(str, "")
}

// VisualLength returns the visual length of a string after removing ANSI color codes
func VisualLength(str string) int {
	return len(StripANSI(str))
}

// PrintBox prints a box with title and content
func PrintBox(title string, content string, boxType int, colors string) {
	chars := BoxChars[boxType]

	// Calculate width based on the longest line in content or title
	lines := append([]string{title}, strings.Split(content, "\n")...)
	width := 0
	for _, line := range lines {
		visualLen := VisualLength(line)
		if visualLen > width {
			width = visualLen
		}
	}
	width += 4 // Add padding

	// Print the box
	ColorPrint(colors, chars[0])                        // Top left corner
	ColorPrint(colors, strings.Repeat(chars[1], width)) // Top border
	ColorPrintln(colors, chars[2])                      // Top right corner

	// Print title if any
	if title != "" {
		ColorPrint(colors, chars[3]) // Left border
		titleVisualLen := VisualLength(title)
		ColorPrint(colors+Bold, fmt.Sprintf(" %s%s ", title, strings.Repeat(" ", width-titleVisualLen-2)))
		ColorPrintln(colors, chars[3]) // Right border

		// Print separator
		ColorPrint(colors, chars[3]) // Left border
		ColorPrint(colors, strings.Repeat(chars[1], width))
		ColorPrintln(colors, chars[3]) // Right border
	}

	// Print content
	for _, line := range strings.Split(content, "\n") {
		ColorPrint(colors, chars[3]) // Left border

		// Calculate padding considering ANSI codes
		visualLen := VisualLength(line)
		padding := strings.Repeat(" ", width-visualLen-2)

		fmt.Printf(" %s%s ", line, padding)
		ColorPrintln(colors, chars[3]) // Right border
	}

	// Print bottom of box
	ColorPrint(colors, chars[4])                        // Bottom left corner
	ColorPrint(colors, strings.Repeat(chars[1], width)) // Bottom border
	ColorPrintln(colors, chars[5])                      // Bottom right corner
}
