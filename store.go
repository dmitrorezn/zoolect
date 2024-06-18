package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

var driver = "sqlite3"

const (
	createTable = `CREATE TABLE IF NOT EXISTS key_value (
    id integer NOT NULL PRIMARY KEY AUTOINCREMENT,
	key text    NOT NULL,
	value text NOT NULL 
)`
)

func Connect() (*sql.DB, error) {
	db, err := sql.Open(driver, "db.db")
	if err != nil {
		return nil, err
	}
	var stm *sql.Stmt
	if stm, err = db.Prepare(createTable); err != nil {
		return nil, err
	}
	if _, err = stm.Exec(); err != nil {
		return nil, err
	}

	return db, nil
}

type Repository struct {
	db DB
}
type DB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (r Repository) Insert(ctx context.Context, cmd Cmd) error {
	result, err := r.db.ExecContext(ctx, `INSERT INTO key_value VALUES(key,value) (?,?) RETURNING *`, cmd.Key, cmd.Value)
	if err != nil {
		return err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	fmt.Println("result", id)

	return nil
}

func (r Repository) Find(ctx context.Context, key string) (val string, err error) {
	row := r.db.QueryRowContext(ctx, `SELECT value FROM key_value WHERE key = ?`, key)
	if err = row.Err(); err != nil {
		return "", err
	}

	return val, row.Scan(&val)
}
