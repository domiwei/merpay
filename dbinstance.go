package mydb

import (
	"database/sql"
	"sync/atomic"
)

type instance struct {
	*sql.DB
	state DBState
}

func NewDBInstance(db *sql.DB) *instance {
	return &instance{
		DB: db,
		state: DBStateDisconnected,
	}
}

// IsAlive simply returns true if cached state is DBStateConnected
func (i *instance) IsAlive() bool {
	return i.state == DBStateConnected
}

// UpdateState atomically updates db state
func (i *instance) UpdateState(oldState, newState DBState) (success bool) {
	return atomic.CompareAndSwapInt32((*int32)(&i.state), int32(oldState), int32(newState))
}

// CheckConnection checks connection state and update it if needed
func (i *instance) CheckConnection() DBState {
	oldState := i.state
	newState := DBStateConnected
	if err := i.DB.Ping(); err != nil {
		newState = DBStateDisconnected
	}
	if oldState != newState {
		i.UpdateState(oldState, newState)
	}
	return newState
}

func (i *instance) GetDB() *sql.DB {
	return i.DB
}
