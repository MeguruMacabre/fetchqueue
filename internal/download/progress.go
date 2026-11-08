package download

import (
	"io"
	"time"
)

type ProgressWriter struct {
	Writer     io.Writer
	Written    int64
	Total      int64
	LastUpdate time.Time
}

func (p *ProgressWriter) Write(data []byte) (n int, err error) {
	n, err = p.Writer.Write(data)
	p.Written += int64(n)

	if time.Since(p.LastUpdate) >= 250*time.Millisecond {
		if p.Total > 0 {
			//fmt.Printf("\rProgress: %.2f%%", p.Progress())
		} else {
			//fmt.Printf("\rDownloaded: %d bytes", p.Written)
		}
		p.LastUpdate = time.Now()
	}
	return n, err
}

func (p *ProgressWriter) Progress() float64 {
	if p.Total <= 0 {
		return 0
	}
	return float64(p.Written) / float64(p.Total) * 100
}

func (p *ProgressWriter) PrintProgress() {
	if p.Total > 0 {
		//fmt.Printf("\rProgress: %.2f%%", p.Progress())
	} else {
		//fmt.Printf("\rDownloaded: %d bytes", p.Written)
	}
}
