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
	"time"

	"github.com/MeguruMacabre/fetchqueue/internal/download"
	"github.com/MeguruMacabre/fetchqueue/internal/storage"
	"github.com/MeguruMacabre/fetchqueue/internal/worker"
)

type progressState struct {
	Downloaded int64
	Total      int64
	Previous   int64
	LastSample time.Time
}

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
	started := make(chan worker.Started, workerCount)
	progress := make(chan worker.Progress, workerCount*4)
	results := make(chan worker.Result, workerCount)

	var wg sync.WaitGroup

	for range workerCount {
		wg.Go(func() {
			worker.Worker(ctx, jobs, started, progress, results)
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
		close(started)
		close(progress)
		close(results)
	}()

	fmt.Printf(
		"Starting %d download(s) with %d worker(s)\n",
		len(selected),
		workerCount,
	)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	active := make(map[int64]*progressState)

	var firstErr error
	var failed bool

	for started != nil || progress != nil || results != nil {
		select {
		case event, ok := <-started:
			if !ok {
				started = nil
				continue
			}

			err = storage.SetStatus(db, event.ID, download.DOWNLOADING)
			if err != nil {
				log.Printf("Job ID: %d start error: %v", event.ID, err)

				if firstErr == nil {
					firstErr = err
				}
			} else {
				active[event.ID] = &progressState{
					Total: -1,
				}
			}

			event.Ack <- err

		case p, ok := <-progress:
			if !ok {
				progress = nil
				continue
			}

			state, exists := active[p.ID]
			if !exists {
				continue
			}

			state.Downloaded = p.Downloaded
			state.Total = p.Total

			if state.LastSample.IsZero() {
				state.Previous = p.Downloaded
				state.LastSample = time.Now()
			}

		case result, ok := <-results:
			if !ok {
				results = nil
				continue
			}

			delete(active, result.ID)

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

		case <-ticker.C:
			now := time.Now()

			for id, state := range active {
				if state.LastSample.IsZero() {
					continue
				}

				elapsed := now.Sub(state.LastSample).Seconds()
				if elapsed <= 0 {
					continue
				}

				speed := float64(state.Downloaded-state.Previous) / elapsed
				if speed < 0 {
					speed = 0
				}

				printProgress(
					id,
					state.Downloaded,
					state.Total,
					speed,
				)

				state.Previous = state.Downloaded
				state.LastSample = now
			}
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

func printProgress(id int64, downloaded, total int64, speed float64) {
	const mib = 1024 * 1024

	downloadedMiB := float64(downloaded) / mib
	speedMiB := speed / mib

	if total <= 0 {
		fmt.Printf(
			"Job ID: %d | %.1f MiB | %.2f MiB/s | ETA unknown\n",
			id,
			downloadedMiB,
			speedMiB,
		)
		return
	}

	percent := float64(downloaded) / float64(total) * 100

	eta := "-"
	if speed > 0 {
		remaining := total - downloaded

		if remaining > 0 {
			eta = fmt.Sprintf("%.0fs", float64(remaining)/speed)
		} else {
			eta = "0s"
		}
	}

	fmt.Printf(
		"Job ID: %d | %.1f%% | %.1f/%.1f MiB | %.2f MiB/s | ETA %s\n",
		id,
		percent,
		downloadedMiB,
		float64(total)/mib,
		speedMiB,
		eta,
	)
}
