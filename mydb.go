package mydb

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

type RWSplitDB struct {
	master       *instance
	readreplicas []*instance
	count        int
}

func NewDB(master *sql.DB, readreplicas ...*sql.DB) DB {
	masterIns := NewDBInstance(master)
	if state := masterIns.CheckConnection(); state != DBStateConnected {
		panic("Cannot connect to only master db")
	}

	// Check state of replicas concurrently
	var wg sync.WaitGroup
	replicaInses := []*instance{}
	for i := range readreplicas {
		replicaIns := NewDBInstance(readreplicas[i])
		replicaInses = append(replicaInses, replicaIns)
		wg.Add(1)
		go func(replicaIns *instance) {
			defer wg.Done()
			replicaIns.CheckConnection()
		}(replicaIns)
	}
	wg.Wait()

	// Reject to init if all replicas are disconnected
	someoneAlive := false
	for _, r := range replicaInses {
		if r.IsAlive() {
			someoneAlive = true
			break
		}
	}
	if !someoneAlive {
		panic("Cannot connect to any replica")
	}

	return &RWSplitDB{
		master:       masterIns,
		readreplicas: replicaInses,
	}
}

func (db *RWSplitDB) readReplicaRoundRobin() *instance {
	db.count++
	return db.readreplicas[db.count%len(db.readreplicas)]
}

func (db *RWSplitDB) Ping() error {
	if err := db.master.Ping(); err != nil {
		panic(err)
	}

	for i := range db.readreplicas {
		if err := db.readreplicas[i].Ping(); err != nil {
			panic(err)
		}
	}

	return nil
}

func (db *RWSplitDB) PingContext(ctx context.Context) error {
	if err := db.master.PingContext(ctx); err != nil {
		panic(err)
	}

	for i := range db.readreplicas {
		if err := db.readreplicas[i].PingContext(ctx); err != nil {
			panic(err)
		}
	}

	return nil
}

func (db *RWSplitDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.readReplicaRoundRobin().Query(query, args...)
}

func (db *RWSplitDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return db.readReplicaRoundRobin().QueryContext(ctx, query, args...)
}

func (db *RWSplitDB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.readReplicaRoundRobin().QueryRow(query, args...)
}

func (db *RWSplitDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return db.readReplicaRoundRobin().QueryRowContext(ctx, query, args...)
}

func (db *RWSplitDB) Begin() (*sql.Tx, error) {
	return db.master.Begin()
}

func (db *RWSplitDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return db.master.BeginTx(ctx, opts)
}

func (db *RWSplitDB) Close() error {
	db.master.Close()
	for i := range db.readreplicas {
		db.readreplicas[i].Close()
	}
	return nil
}

func (db *RWSplitDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.master.Exec(query, args...)
}

func (db *RWSplitDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.master.ExecContext(ctx, query, args...)
}

func (db *RWSplitDB) Prepare(query string) (*sql.Stmt, error) {
	return db.master.Prepare(query)
}

func (db *RWSplitDB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return db.master.PrepareContext(ctx, query)
}

func (db *RWSplitDB) SetConnMaxLifetime(d time.Duration) {
	db.master.SetConnMaxLifetime(d)
	for i := range db.readreplicas {
		db.readreplicas[i].SetConnMaxLifetime(d)
	}
}

func (db *RWSplitDB) SetMaxIdleConns(n int) {
	db.master.SetMaxIdleConns(n)
	for i := range db.readreplicas {
		db.readreplicas[i].SetMaxIdleConns(n)
	}
}

func (db *RWSplitDB) SetMaxOpenConns(n int) {
	db.master.SetMaxOpenConns(n)
	for i := range db.readreplicas {
		db.readreplicas[i].SetMaxOpenConns(n)
	}
}
