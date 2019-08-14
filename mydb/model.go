package mydb

type DBState int32

const (
	DBStateDisconnected DBState = iota
	DBStateConnected
)

var (
	stateStr = map[DBState]string{
		DBStateDisconnected: "DBStateDisconnected",
		DBStateConnected:    "DBStateConnected",
	}
)
