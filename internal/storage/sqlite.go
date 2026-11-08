package storage

import (
	"database/sql"
	"fmt"

	"github.com/MeguruMacabre/fetchqueue/internal/download"
	_ "modernc.org/sqlite"
)

const DefaultWorkers = 3

func Open(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func Migrate(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS downloads (
		id INTEGER PRIMARY KEY,
		url TEXT NOT NULL,
		filename TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'PENDING',
		downloaded INTEGER NOT NULL DEFAULT 0,
		total INTEGER NOT NULL DEFAULT -1
	)`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS settings (
		key TEXT PRIMARY KEY,
		value INTEGER NOT NULL
	)`)
	if err != nil {
		return err
	}

	_, err = db.Exec(
		"INSERT OR IGNORE INTO settings (key, value) VALUES ('workers', ?)",
		DefaultWorkers,
	)
	if err != nil {
		return err
	}

	_, err = db.Exec("UPDATE downloads SET status = UPPER(status)")
	if err != nil {
		return err
	}

	return nil
}

func AddDownload(db *sql.DB, url, filename string) (int64, error) {
	result, err := db.Exec(
		"INSERT INTO downloads (url, filename) VALUES (?, ?)",
		url,
		filename,
	)
	if err != nil {
		return 0, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return id, nil
}

func GetDownloads(db *sql.DB) ([]download.Download, error) {
	rows, err := db.Query(
		"SELECT id, url, filename, status, downloaded, total FROM downloads ORDER BY id",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	downloads := make([]download.Download, 0)

	for rows.Next() {
		var d download.Download

		err = rows.Scan(
			&d.ID,
			&d.URL,
			&d.Filename,
			&d.Status,
			&d.Downloaded,
			&d.Total,
		)
		if err != nil {
			return nil, err
		}

		downloads = append(downloads, d)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return downloads, nil
}

func UpdateResult(
	db *sql.DB,
	id int64,
	status string,
	downloaded, total int64,
) error {
	result, err := db.Exec(
		"UPDATE downloads SET status = ?, downloaded = ?, total = ? WHERE id = ?",
		status,
		downloaded,
		total,
		id,
	)
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if affected == 0 {
		return fmt.Errorf("download with id %d not found", id)
	}

	return nil
}

func ClearDownloads(db *sql.DB) error {
	_, err := db.Exec("DELETE FROM downloads")
	return err
}

func GetWorkerCount(db *sql.DB) (int, error) {
	var count int

	err := db.QueryRow(
		"SELECT value FROM settings WHERE key = 'workers'",
	).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func SetWorkerCount(db *sql.DB, count int) error {
	if count < 1 || count > 32 {
		return fmt.Errorf("worker count must be between 1 and 32")
	}

	_, err := db.Exec(
		`INSERT INTO settings (key, value)
		 VALUES ('workers', ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		count,
	)

	return err
}

func SetStatus(db *sql.DB, id int64, status string) error {
	result, err := db.Exec("UPDATE downloads SET status = ? WHERE id = ?", status, id)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return fmt.Errorf("download with id %d not found", id)
	}
	return nil
}

func RecoverDownloads(db *sql.DB) error {
	_, err := db.Exec(`
		UPDATE downloads
		SET status = 'PAUSED'
		WHERE status = 'DOWNLOADING'
	`)
	return err
}

func DeleteDownload(db *sql.DB, id int64) (string, error) {
	var filename string

	err := db.QueryRow(
		"DELETE FROM downloads WHERE id = ? RETURNING filename",
		id,
	).Scan(&filename)

	if err == sql.ErrNoRows {
		return "", fmt.Errorf("download with id %d not found", id)
	}

	if err != nil {
		return "", err
	}

	return filename, nil
}
