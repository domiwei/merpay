All the go files relevent to implementation of read-write splitting DB are put under this directory.
- db.go defines the DB interface to be implemented.
- dbinstance.go defines a wrapper structure of sql.DB in golang with cached state, and some connection check function.
- model.go defines connection state used in this package.
- mydb.go is the main file that implements lots of methods defined in the interface in db.go.
- mydb_test.go is unittest for methods defined in mydb.go.
- rand.go defines an auxiliary rand function used in this pkg.
