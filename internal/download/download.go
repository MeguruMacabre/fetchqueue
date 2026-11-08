package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	PENDING     = "PENDING"
	DOWNLOADING = "DOWNLOADING"
	COMPLETED   = "COMPLETED"
	PAUSED      = "PAUSED"
	FAILED      = "FAILED"
)

type Download struct {
	ID         int64
	URL        string
	Filename   string
	Status     string
	Downloaded int64
	Total      int64
}

func DownloadFile(
	ctx context.Context,
	filePath string,
	url string,
	onProgress func(downloaded, total int64),
) (Download, error) {
	result := Download{
		URL:      url,
		Filename: filepath.Base(filePath),
		Status:   FAILED,
		Total:    -1,
	}

	var downloaded int64

	info, err := os.Stat(filePath)
	if err == nil {
		if !info.Mode().IsRegular() {
			return result, fmt.Errorf("download path is not a regular file")
		}

		downloaded = info.Size()
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, fmt.Errorf("could not stat file: %w", err)
	}

	result.Downloaded = downloaded

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return result, fmt.Errorf("could not create HTTP request: %w", err)
	}

	req.Header.Set("Accept-Encoding", "identity")

	if downloaded > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", downloaded))
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			result.Status = PAUSED
		}
		return result, fmt.Errorf("could not download file: %w", err)
	}
	defer resp.Body.Close()

	encoding := resp.Header.Get("Content-Encoding")
	if encoding != "" && !strings.EqualFold(encoding, "identity") {
		return result, fmt.Errorf("unsupported content encoding: %s", encoding)
	}

	var expectedBytes int64

	switch resp.StatusCode {
	case http.StatusOK:
		if downloaded > 0 {
			return result, fmt.Errorf("server ignored Range request")
		}

		result.Total = resp.ContentLength
		expectedBytes = resp.ContentLength

	case http.StatusPartialContent:
		if downloaded == 0 {
			return result, fmt.Errorf("unexpected partial response")
		}

		start, end, total, err := parseContentRange(
			resp.Header.Get("Content-Range"),
		)
		if err != nil {
			return result, fmt.Errorf("invalid Content-Range: %w", err)
		}

		if start != downloaded {
			return result, fmt.Errorf(
				"unexpected range start: expected %d, got %d",
				downloaded,
				start,
			)
		}

		expectedBytes = end - start + 1
		result.Total = total

		if resp.ContentLength >= 0 && resp.ContentLength != expectedBytes {
			return result, fmt.Errorf("unexpected response length")
		}

	default:
		return result, fmt.Errorf("unexpected HTTP status: %s", resp.Status)
	}

	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if downloaded > 0 {
		flags = os.O_WRONLY | os.O_APPEND
	}

	out, err := os.OpenFile(filePath, flags, 0644)
	if err != nil {
		return result, fmt.Errorf("could not open file: %w", err)
	}

	writer := &ProgressWriter{
		Writer:     out,
		Offset:     downloaded,
		Total:      result.Total,
		OnProgress: onProgress,
	}

	written, copyErr := io.Copy(writer, resp.Body)
	result.Downloaded = downloaded + written

	closeErr := out.Close()

	if copyErr != nil || closeErr != nil {
		if ctx.Err() != nil && closeErr == nil {
			result.Status = PAUSED
		}

		return result, fmt.Errorf(
			"could not write file: %w",
			errors.Join(copyErr, closeErr),
		)
	}

	if expectedBytes >= 0 && written != expectedBytes {
		return result, fmt.Errorf(
			"incomplete response: expected %d bytes, got %d",
			expectedBytes,
			written,
		)
	}

	if result.Total >= 0 && result.Downloaded != result.Total {
		return result, fmt.Errorf(
			"incomplete download: expected %d bytes, got %d",
			result.Total,
			result.Downloaded,
		)
	}

	result.Status = COMPLETED
	return result, nil
}

func parseContentRange(value string) (int64, int64, int64, error) {
	if !strings.HasPrefix(value, "bytes ") {
		return 0, 0, 0, fmt.Errorf("invalid range unit")
	}

	value = strings.TrimPrefix(value, "bytes ")

	rangePart, totalPart, ok := strings.Cut(value, "/")
	if !ok {
		return 0, 0, 0, fmt.Errorf("missing total size")
	}

	startPart, endPart, ok := strings.Cut(rangePart, "-")
	if !ok {
		return 0, 0, 0, fmt.Errorf("invalid byte range")
	}

	start, err := strconv.ParseInt(startPart, 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}

	end, err := strconv.ParseInt(endPart, 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}

	total, err := strconv.ParseInt(totalPart, 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}

	if start < 0 || end < start || total <= end {
		return 0, 0, 0, fmt.Errorf("invalid range values")
	}

	return start, end, total, nil
}
