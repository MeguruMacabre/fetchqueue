package worker

import (
	"context"
	"fmt"

	"github.com/MeguruMacabre/fetchqueue/internal/download"
)

type Job struct {
	ID       int64
	FilePath string
	URL      string
}

type Started struct {
	ID  int64
	Ack chan error
}

type Progress struct {
	ID         int64
	Downloaded int64
	Total      int64
}

type Result struct {
	ID       int64
	Download download.Download
	Err      error
}

func Worker(
	ctx context.Context,
	jobs <-chan Job,
	started chan<- Started,
	progress chan<- Progress,
	results chan<- Result,
) {
	for {
		select {
		case <-ctx.Done():
			return

		case job, ok := <-jobs:
			if !ok {
				return
			}

			if ctx.Err() != nil {
				return
			}

			event := Started{
				ID:  job.ID,
				Ack: make(chan error, 1),
			}

			select {
			case started <- event:
			case <-ctx.Done():
				return
			}

			err := <-event.Ack
			if err != nil {
				fmt.Printf("Job ID: %d Start failed: %v\n", job.ID, err)
				continue
			}

			fmt.Printf("Job ID: %d Started\n", job.ID)

			onProgress := func(downloaded, total int64) {
				update := Progress{
					ID:         job.ID,
					Downloaded: downloaded,
					Total:      total,
				}

				select {
				case progress <- update:
				default:
				}
			}

			result, err := download.DownloadFile(
				ctx,
				job.FilePath,
				job.URL,
				onProgress,
			)

			results <- Result{
				ID:       job.ID,
				Download: result,
				Err:      err,
			}
		}
	}
}
