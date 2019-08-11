package mydb

import (
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// RandPosInt returns random positive 32 bits int
func RandPosInt() int32 {
	return rand.Int31()
}
