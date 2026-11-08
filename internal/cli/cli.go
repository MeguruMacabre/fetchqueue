package cli

import (
	"bufio"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/MeguruMacabre/fetchqueue/internal/download"
	"github.com/MeguruMacabre/fetchqueue/internal/queue"
	"github.com/MeguruMacabre/fetchqueue/internal/storage"
)

type App struct {
	db           *sql.DB
	downloadsDir string
	scanner      *bufio.Scanner
}

func Run(args []string) error {
	if len(args) == 1 && args[0] == "help" {
		printHelp()
		return nil
	}

	exePath, err := os.Executable()
	if err != nil {
		return err
	}

	exeDir := filepath.Dir(exePath)
	downloadsDir := filepath.Join(exeDir, "downloads")
	dbPath := filepath.Join(exeDir, "fetchqueue.db")

	err = os.MkdirAll(downloadsDir, 0755)
	if err != nil {
		return err
	}

	db, err := storage.Open(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	err = storage.Migrate(db)
	if err != nil {
		return err
	}

	app := &App{
		db:           db,
		downloadsDir: downloadsDir,
		scanner:      bufio.NewScanner(os.Stdin),
	}

	if len(args) > 0 {
		return app.execute(args)
	}

	return app.interactive()
}

func (a *App) interactive() error {
	fmt.Println("FetchQueue v0.1.0")
	fmt.Println("Type help for available commands.")

	for {
		fmt.Print("\nfetchqueue> ")

		if !a.scanner.Scan() {
			return a.scanner.Err()
		}

		input := strings.TrimSpace(a.scanner.Text())
		if input == "" {
			continue
		}

		args := strings.Fields(input)

		switch args[0] {
		case "exit", "quit":
			fmt.Println("Goodbye!")
			return nil

		default:
			err := a.execute(args)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			}
		}
	}
}

func (a *App) execute(args []string) error {
	if len(args) == 0 {
		return nil
	}

	switch args[0] {
	case "help":
		printHelp()
		return nil

	case "add":
		return a.add(args)

	case "list":
		if len(args) != 1 {
			return fmt.Errorf("usage: list")
		}
		return a.list()

	case "workers":
		return a.workers(args)

	case "start", "resume", "retry":
		if len(args) != 1 {
			return fmt.Errorf("usage: %s", args[0])
		}

		var status string

		switch args[0] {
		case "start":
			status = download.PENDING
		case "resume":
			status = download.PAUSED
		case "retry":
			status = download.FAILED
		}

		count, err := storage.GetWorkerCount(a.db)
		if err != nil {
			return err
		}

		return queue.Run(a.db, a.downloadsDir, count, status)

	case "clear":
		if len(args) != 1 {
			return fmt.Errorf("usage: clear")
		}
		return a.clear()

	default:
		return fmt.Errorf("unknown command: %s", args[0])
	}
}

func (a *App) add(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: add <url> [filename]")
	}

	parsed, err := url.ParseRequestURI(args[1])
	if err != nil {
		return err
	}

	if (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Hostname() == "" {
		return fmt.Errorf("only HTTP and HTTPS URLs are supported")
	}

	filename := defaultFilename(parsed.Path)

	if len(args) > 2 {
		filename = strings.Join(args[2:], " ")

		if strings.HasPrefix(filename, "\"") &&
			strings.HasSuffix(filename, "\"") {
			filename, err = strconv.Unquote(filename)
			if err != nil {
				return err
			}
		}
	}

	if filename == "" || filename == "." || filename == ".." ||
		strings.ContainsAny(filename, "/\\\x00\r\n") {
		return fmt.Errorf("invalid filename")
	}

	id, err := storage.AddDownload(a.db, args[1], filename)
	if err != nil {
		return err
	}

	fmt.Printf("Added download ID: %d | %s\n", id, filename)
	return nil
}

func defaultFilename(urlPath string) string {
	if urlPath == "" || strings.HasSuffix(urlPath, "/") {
		return "download.bin"
	}

	filename := path.Base(urlPath)

	if filename == "" || filename == "." || filename == "/" {
		return "download.bin"
	}

	return filename
}

func (a *App) list() error {
	downloads, err := storage.GetDownloads(a.db)
	if err != nil {
		return err
	}

	if len(downloads) == 0 {
		fmt.Println("No downloads")
		return nil
	}

	for _, d := range downloads {
		fmt.Printf(
			"ID: %d | %-11s | %d/%d bytes | %s\n",
			d.ID,
			d.Status,
			d.Downloaded,
			d.Total,
			d.Filename,
		)
	}

	return nil
}

func (a *App) workers(args []string) error {
	if len(args) == 1 {
		count, err := storage.GetWorkerCount(a.db)
		if err != nil {
			return err
		}

		fmt.Printf("Workers: %d\n", count)
		return nil
	}

	if len(args) != 2 {
		return fmt.Errorf("usage: workers [count]")
	}

	count, err := strconv.Atoi(args[1])
	if err != nil {
		return fmt.Errorf("invalid worker count: %w", err)
	}

	err = storage.SetWorkerCount(a.db, count)
	if err != nil {
		return err
	}

	fmt.Printf("Workers set to %d\n", count)
	return nil
}

func (a *App) clear() error {
	fmt.Print("Delete ALL downloads and database records? Type CLEAR: ")

	if !a.scanner.Scan() {
		err := a.scanner.Err()
		if err != nil {
			return err
		}
		return fmt.Errorf("confirmation was not provided")
	}

	input := strings.TrimSpace(a.scanner.Text())

	if input != "CLEAR" {
		fmt.Println("Cancelled")
		return nil
	}

	err := os.RemoveAll(a.downloadsDir)
	if err != nil {
		return err
	}

	err = os.MkdirAll(a.downloadsDir, 0755)
	if err != nil {
		return err
	}

	err = storage.ClearDownloads(a.db)
	if err != nil {
		return err
	}

	fmt.Println("Downloads and database records cleared")
	return nil
}

func printHelp() {
	fmt.Println("FetchQueue")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  add <url> [filename]  Add a download")
	fmt.Println("  list                  List downloads")
	fmt.Println("  start                 Start pending downloads")
	fmt.Println("  resume                Resume paused downloads")
	fmt.Println("  retry                 Retry failed downloads")
	fmt.Println("  workers [count]       Show or change worker count (1-32)")
	fmt.Println("  clear                 Clear downloads and database records")
	fmt.Println("  help                  Show this help")
	fmt.Println("  exit                  Exit interactive mode")
}
