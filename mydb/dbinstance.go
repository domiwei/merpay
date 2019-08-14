package mydb

import (
	"database/sql"
	"sync/atomic"
	"time"

	"github.com/prometheus/common/log"
)

var (
	diffCheckingTime = int64(5)
)

// instance is a wapper of sql.DB so as to cache the connection state
// of this DB.
type instance struct {
	*sql.DB
	instanceName string
	// Cached DB state
	state DBState
	// Last checking timestamp
	lastCheckTime int64
	// It's a mutex to prevent ping action from cache avalanch
	pingLock uint32
}

func NewDBInstance(db *sql.DB, name string) *instance {
	return &instance{
		DB:           db,
		instanceName: name,
		state:        DBStateDisconnected,
	}
}

// IsAlive simply returns true if cached state is DBStateConnected
func (i *instance) IsAlive() bool {
	return i.safeGetState() == DBStateConnected
}

// CheckConnection checks connection state and update it if needed.
// This function is thread-safe. Only one thread is allowed to ping DB anytime.
func (i *instance) CheckConnection() DBState {
	t := atomic.LoadInt64(&i.lastCheckTime)
	if time.Now().Unix()-t < diffCheckingTime {
		// Skip to check if last check time within 5 sec
		return i.safeGetState()
	}

	// Try lock so that only one thread is allowed to ping DB at a time.
	// Note it's important to lock here to prevent this action from cache avalanche.
	if !atomic.CompareAndSwapUint32(&i.pingLock, 0, 1) {
		return i.safeGetState()
	}
	// Remember to reset the lock
	defer atomic.StoreUint32(&i.pingLock, 0)
	oldState := i.safeGetState()
	newState := DBStateConnected
	if err := i.DB.Ping(); err != nil {
		newState = DBStateDisconnected
	}
	if oldState != newState {
		if i.updateState(oldState, newState) {
			log.Infof("Change state of %s from %s to %s", i.instanceName, stateStr[oldState], stateStr[newState])
		}
	}
	// Update check time
	i.lastCheckTime = time.Now().Unix()
	return newState
}

// updateState atomically updates db state
func (i *instance) updateState(oldState, newState DBState) (success bool) {
	return atomic.CompareAndSwapInt32((*int32)(&i.state), int32(oldState), int32(newState))
}

// safeGetState utilizes atomic load function to get state without data racing.
func (i *instance) safeGetState() DBState {
	return DBState(atomic.LoadInt32((*int32)(&i.state)))
}
