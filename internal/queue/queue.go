package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"

	"github.com/MeguruMacabre/fetchqueue/internal/download"
	"github.com/MeguruMacabre/fetchqueue/internal/storage"
	"github.com/MeguruMacabre/fetchqueue/internal/worker"
)

func Run(
	db *sql.DB,
	downloadsDir string,
	workerCount int,
	wantedStatus string,
) error {
	if workerCount < 1 || workerCount > 32 {
		return fmt.Errorf("invalid worker count: %d", workerCount)
	}

	downloads, err := storage.GetDownloads(db)
	if err != nil {
		return err
	}

	selected := make([]download.Download, 0)

	for _, d := range downloads {
		if d.Status == wantedStatus {
			selected = append(selected, d)
		}
	}

	if len(selected) == 0 {
		fmt.Printf("No downloads with status %s\n", wantedStatus)
		return nil
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
	)
	defer stop()

	jobs := make(chan worker.Job, workerCount)
	results := make(chan worker.Result, workerCount)

	var wg sync.WaitGroup

	for range workerCount {
		wg.Go(func() {
			worker.Worker(ctx, jobs, results)
		})
	}

	go func() {
		defer close(jobs)

		for _, d := range selected {
			job := worker.Job{
				ID: d.ID,
				FilePath: filepath.Join(
					downloadsDir,
					fmt.Sprintf("%d_%s", d.ID, filepath.Base(d.Filename)),
				),
				URL: d.URL,
			}

			select {
			case jobs <- job:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	fmt.Printf(
		"Starting %d download(s) with %d worker(s)\n",
		len(selected),
		workerCount,
	)

	var firstErr error
	var failed bool

	for result := range results {
		var status string

		switch {
		case result.Err == nil:
			status = download.COMPLETED

		case result.Download.Status == download.PAUSED ||
			errors.Is(result.Err, context.Canceled):
			status = download.PAUSED

		default:
			status = download.FAILED
		}

		err = storage.UpdateResult(
			db,
			result.ID,
			status,
			result.Download.Downloaded,
			result.Download.Total,
		)
		if err != nil {
			log.Printf("Job ID: %d database error: %v", result.ID, err)

			if firstErr == nil {
				firstErr = err
			}
		}

		if result.Err != nil {
			log.Printf("Job ID: %d %s: %v", result.ID, status, result.Err)
		} else {
			log.Printf("Job ID: %d %s", result.ID, status)
		}

		if status == download.FAILED {
			failed = true
		}
	}

	if firstErr != nil {
		return firstErr
	}

	if failed {
		return fmt.Errorf("one or more downloads failed")
	}

	return nil
}
