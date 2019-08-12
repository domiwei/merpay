package mydb

import (
	"database/sql"
	"sync/atomic"
	"time"
)

type instance struct {
	*sql.DB
	// Cached DB state
	state DBState
	// Last checking timestamp
	lastCheckTime int64
	pingLock      uint32
}

func NewDBInstance(db *sql.DB) *instance {
	return &instance{
		DB:    db,
		state: DBStateDisconnected,
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
	if time.Now().Unix()-t < 5 {
		// Skip to check if last check time within 5 sec
		return i.safeGetState()
	}

	// Try lock so that only one thread is allowed to ping DB at a time.
	if !atomic.CompareAndSwapUint32(&i.pingLock, 0, 1) {
		return i.safeGetState()
	}
	defer atomic.StoreUint32(&i.pingLock, 0)
	oldState := i.safeGetState()
	newState := DBStateConnected
	if err := i.DB.Ping(); err != nil {
		newState = DBStateDisconnected
	}
	if oldState != newState {
		i.updateState(oldState, newState)
	}
	// Update check time
	i.lastCheckTime = time.Now().Unix()
	return newState
}

// updateState atomically updates db state
func (i *instance) updateState(oldState, newState DBState) (success bool) {
	return atomic.CompareAndSwapInt32((*int32)(&i.state), int32(oldState), int32(newState))
}

func (i *instance) safeGetState() DBState {
	return DBState(atomic.LoadInt32((*int32)(&i.state)))
}
