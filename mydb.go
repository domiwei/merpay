package mydb

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

var (
	ErrDisconnected = fmt.Errorf("DB was disconnected")
)

type RWSplitDB struct {
	master       *instance
	readreplicas []*instance
	count        int
}

func NewDB(master *sql.DB, readreplicas ...*sql.DB) (DB, error) {
	// Wrap master and replicas by using instance struct
	masterIns := NewDBInstance(master)
	if state := masterIns.CheckConnection(); state != DBStateConnected {
		return nil, fmt.Errorf("Cannot connect to master db")
	}
	replicaInses := []*instance{}
	for i := range readreplicas {
		replicaIns := NewDBInstance(readreplicas[i])
		replicaInses = append(replicaInses, replicaIns)
	}
	db := &RWSplitDB{
		master:       masterIns,
		readreplicas: replicaInses,
	}

	// Check connection state for each replica
	disConnCount := 0
	for err := range db.concurrentlyDo(checkConn) {
		if err != nil {
			disConnCount++
		}
	}
	// Reject if all replicas are disconnected
	if disConnCount == len(replicaInses) {
		return nil, fmt.Errorf("Cannot connect to any replica")
	}

	return db, nil
}

func (db *RWSplitDB) readReplicaRoundRobin() *instance {
	db.count++
	return db.readreplicas[db.count%len(db.readreplicas)]
}

func (db *RWSplitDB) Ping() error {
	if err := db.master.Ping(); err != nil {
		fmt.Errorf(err.Error())
		return err
	}

	// Ping replica concurrently
	var err error
	ping := func(dbIns *instance) error {
		return dbIns.Ping()
	}
	for result := range db.concurrentlyDo(ping) {
		if result != nil {
			err = result
			fmt.Errorf(err.Error())
		}
	}
	return err
}

func (db *RWSplitDB) PingContext(ctx context.Context) error {
	if err := db.master.PingContext(ctx); err != nil {
		fmt.Errorf(err.Error())
		return err
	}

	// Ping replica concurrently
	var err error
	ping := func(dbIns *instance) error {
		return dbIns.PingContext(ctx)
	}
	for result := range db.concurrentlyDo(ping) {
		if result != nil {
			err = result
			fmt.Errorf(err.Error())
		}
	}
	return err
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

// concurrentlyDo invoke f for each replica concurrently. It returns a channel
// to let caller receive the result of pre-defined function.
func (db *RWSplitDB) concurrentlyDo(f func(dbIns *instance) error) <-chan error {
	resultChan := make(chan error)
	go func() {
		var wg sync.WaitGroup
		for _, replica := range db.readreplicas {
			wg.Add(1)
			go func(replica *instance) {
				defer wg.Done()
				// Pass result to channel
				resultChan <- f(replica)
			}(replica)
		}
		wg.Wait()
		close(resultChan)
	}()
	return resultChan
}

// checkConn is a helper func to check connection state. This function is passed as
// an argument of concurrentlyDo.
func checkConn(dbIns *instance) error {
	if dbIns.CheckConnection() != DBStateConnected {
		return ErrDisconnected
	}
	return nil
}
