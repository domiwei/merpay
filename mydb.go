package mydb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/common/log"
)

var (
	ErrDisconnected       = errors.New("DB was disconnected")
	periodicallyCheckTime = 5 * time.Minute
)

type RWSplitDB struct {
	master           *instance
	readreplicas     []*instance
	numReplica       int
	count            int
	toBeCheckIdxChan chan int
	shutDownChan     chan struct{}
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
		master:           masterIns,
		readreplicas:     replicaInses,
		numReplica:       len(replicaInses),
		toBeCheckIdxChan: make(chan int, len(replicaInses)*2),
		shutDownChan:     make(chan struct{}, 1),
	}

	// Check connection state for each replica
	disConnCount := 0
	for err := range db.concurrentlyDo(checkConn) {
		if err != nil {
			disConnCount++
		}
	}
	// Reject if all replicas are disconnected
	if disConnCount == db.numReplica {
		return nil, fmt.Errorf("Cannot connect to any replica")
	}

	// Launch a checker do periodical state checking
	go db.replicaChecker()

	return db, nil
}

func (db *RWSplitDB) Ping() error {
	if err := db.master.Ping(); err != nil {
		log.Errorf("Ping master failed. Error %s", err.Error())
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
			log.Errorf("Ping replica failed. Error %s", err.Error())
		}
	}
	return err
}

func (db *RWSplitDB) PingContext(ctx context.Context) error {
	if err := db.master.PingContext(ctx); err != nil {
		log.Errorf("Ping master failed. Error %s", err.Error())
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
			log.Errorf("Ping replica failed. Error %s", err.Error())
		}
	}
	return err
}

func (db *RWSplitDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	var rows *sql.Rows
	var err error
	// Query by choosing any alive replica
	if err = db.readReplicaRandomRoundRobin(func(dbIns *instance) (queryErr error) {
		rows, queryErr = dbIns.Query(query, args...)
		return
	}); err != nil {
		log.Errorf("Query failed. Error %s", err.Error())
	}
	return rows, err
}

func (db *RWSplitDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	var rows *sql.Rows
	var err error
	// Query by choosing any alive replica
	if err = db.readReplicaRandomRoundRobin(func(dbIns *instance) (queryErr error) {
		rows, queryErr = dbIns.QueryContext(ctx, query, args...)
		return
	}); err != nil {
		log.Errorf("Query failed. Error %s", err.Error())
	}
	return rows, err
}

func (db *RWSplitDB) QueryRow(query string, args ...interface{}) *Row {
	// Do nothing but return a wrapper so that defering to query until user invoke Scan()
	return &Row{query: query, args: args, ctx: nil, db: db}
}

func (db *RWSplitDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row {
	// Do nothing but return a wrapper so that defering to query until user invoke Scan()
	return &Row{query: query, args: args, ctx: ctx, db: db}
}

func (db *RWSplitDB) Begin() (*sql.Tx, error) {
	return db.master.Begin()
}

func (db *RWSplitDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return db.master.BeginTx(ctx, opts)
}

func (db *RWSplitDB) Close() error {
	// First notify to shutdown
	db.shutDownChan <- struct{}{}
	// Close all
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

func (db *RWSplitDB) readReplicaRandomRoundRobin(f func(dbIns *instance) error) error {
	err := ErrDisconnected
	numReplica := int32(db.numReplica)
	// Pick up any random index of read-replica, and try to invoke callback func.
	// Once it failed for any reason, move on to next replica in replica pool.
	randIndex := RandPosInt() % numReplica
	for i := randIndex; i < randIndex+numReplica; i++ {
		idx := i % numReplica
		// First check if this db is still alive or not
		if db.readreplicas[idx].IsAlive() {
			if err = f(db.readreplicas[idx]); err != nil {
				// notify monitor to update state of this replica
				db.notifyCheckReplica(int(idx))
				log.Errorf("Error on readReplica[%d]. Error: %s", idx, err.Error())
				continue
			}
			// On success, break the loop and return nil
			break
		}
	}
	return err
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

// notifyCheckReplica notifies replica checker to check state of this replica.
func (db *RWSplitDB) notifyCheckReplica(idx int) {
	select {
	case db.toBeCheckIdxChan <- int(idx):
	default:
		// Skip it while buffer is full
	}
}

func (db *RWSplitDB) replicaChecker() {
	// This func runs in a infinite for loop to periodically
	// check connection state until db is closed.
	for {
		select {
		case <-time.After(periodicallyCheckTime):
			for range db.concurrentlyDo(checkConn) {
			}
		case idx := <-db.toBeCheckIdxChan:
			// Someone reports a replica is disconnected, so check it.
			db.readreplicas[idx].CheckConnection()
		case <-db.shutDownChan:
			return
		}
	}
}

// checkConn is a helper func to check connection state. This function is passed as
// an argument of concurrentlyDo.
func checkConn(dbIns *instance) error {
	if dbIns.CheckConnection() != DBStateConnected {
		return ErrDisconnected
	}
	return nil
}

// Row is an auxiliary structure to imitate the fluent action Scan() coming after QueryRow()
// defined in original sql interface in golang. By doing so, the action QueryRow().Scan()
// is still allowed to perform while using this ReadWriteSplit DB driver.
type Row struct {
	query string
	args  []interface{}
	ctx   context.Context
	db    *RWSplitDB
}

func (r *Row) Scan(dest ...interface{}) error {
	var err error
	if err = r.db.readReplicaRandomRoundRobin(func(dbIns *instance) (queryErr error) {
		if r.ctx != nil {
			queryErr = dbIns.QueryRowContext(r.ctx, r.query, r.args...).Scan(dest...)
		} else {
			queryErr = dbIns.QueryRow(r.query, r.args...).Scan(dest...)
		}
		return
	}); err != nil {
		log.Errorf("Query failed. Error %s", err.Error())
	}
	return err
}
