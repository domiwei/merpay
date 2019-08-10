package mydb

type DBState int32

const (
	DBStateDisconnected DBState = iota
	DBStateConnected
)
