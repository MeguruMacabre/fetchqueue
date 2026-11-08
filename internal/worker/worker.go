package worker

import (
	"context"
	"fmt"

	"github.com/MeguruMacabre/fetchqueue/internal/download"
)

type Result struct {
	ID       int64
	Download download.Download
	Err      error
}

type Job struct {
	ID       int64
	FilePath string
	URL      string
}

func Worker(ctx context.Context, jobs <-chan Job, results chan<- Result) {
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

			fmt.Printf("Job ID: %d Started\n", job.ID)

			result, err := download.DownloadFile(ctx, job.FilePath, job.URL)

			results <- Result{
				ID:       job.ID,
				Download: result,
				Err:      err,
			}
		}
	}
}
