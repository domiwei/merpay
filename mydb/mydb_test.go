package mydb

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/suite"
)

const (
	numReplica = 20
)

type mydbSuite struct {
	suite.Suite
	master      *sql.DB
	mockMaster  sqlmock.Sqlmock
	replica     []*sql.DB
	mockReplica []sqlmock.Sqlmock
	mydb        DB
}

func (s *mydbSuite) SetupSuite() {
	// Always check connection while any request of connection checking coming.
	diffCheckingTime = int64(0)
}

func (s *mydbSuite) SetupTest() {
	// init master
	master, mockMaster, err := sqlmock.New()
	s.Require().NoError(err)
	s.master = master
	s.mockMaster = mockMaster
	// init mock replicas
	s.replica = []*sql.DB{}
	s.mockReplica = []sqlmock.Sqlmock{}
	for i := 0; i < numReplica; i++ {
		replica, mockReplica, err := sqlmock.New()
		s.Require().NoError(err)
		s.replica = append(s.replica, replica)
		s.mockReplica = append(s.mockReplica, mockReplica)
	}
	s.mydb, err = NewDB(s.master, s.replica...)
	s.Require().NoError(err)
}

func (s *mydbSuite) TearDownTest() {
	s.NoError(s.mockMaster.ExpectationsWereMet())
	for i := 0; i < numReplica; i++ {
		s.NoError(s.mockReplica[i].ExpectationsWereMet())
	}
	s.mydb.Close()
}

func (s *mydbSuite) TearDownSuite() {
}

func (s *mydbSuite) TestNewDB() {
	tests := []struct {
		Desc    string
		Prepare func()
		Success bool
	}{
		{
			Desc:    "Success",
			Prepare: func() {},
			Success: true,
		},
		{
			Desc: "Still Success, but some replicas disconn",
			Prepare: func() {
				for i := 0; i < numReplica-1; i++ {
					s.replica[i].Close()
				}
			},
			Success: true,
		},
		{
			Desc: "Failed because of disconnection of master ",
			Prepare: func() {
				s.master.Close()
			},
			Success: false,
		},
		{
			Desc: "Failed because of disconnection of all replicas",
			Prepare: func() {
				s.master.Close()
				for i := 0; i < numReplica; i++ {
					s.replica[i].Close()
				}
			},
			Success: false,
		},
	}

	for _, test := range tests {
		// Reset the test env
		s.TearDownTest()
		s.SetupTest()
		// Prepare before run
		test.Prepare()
		_, err := NewDB(s.master, s.replica...)
		if test.Success {
			s.Require().NoError(err)
		} else {
			s.Require().Error(err)
		}
	}
}

func (s *mydbSuite) TestPing() {
	// Close some replicas except for last one
	for i := 0; i < numReplica-1; i++ {
		s.replica[i].Close()
	}
	s.Require().NoError(s.mydb.Ping())

	// Then close last one. We will get error later
	s.replica[numReplica-1].Close()
	s.Require().Error(s.mydb.Ping())
}

func (s *mydbSuite) TestQuery() {
	// Close some replicas except for last one
	for i := 0; i < numReplica-1; i++ {
		s.replica[i].Close()
	}

	// Prepare mock result
	returnRows := sqlmock.NewRows([]string{"name", "age"}).AddRow("kewei", 30)
	s.mockReplica[numReplica-1].ExpectQuery("^SELECT (.+) FROM table").WithArgs("kewei").WillReturnRows(returnRows)
	// Run
	rows, err := s.mydb.Query("SELECT * FROM table WHERE name=?", "kewei")
	defer rows.Close()
	// Check result
	var name string
	var age int
	for rows.Next() {
		s.Require().NoError(rows.Scan(&name, &age))
		s.Equal("kewei", name)
		s.Equal(30, age)
	}
	s.Require().Equal(err, nil)
}

func (s *mydbSuite) TestQueryContext() {
	// Close some replicas except for last one
	for i := 0; i < numReplica-1; i++ {
		s.replica[i].Close()
	}

	// Prepare mock result
	returnRows := sqlmock.NewRows([]string{"name", "age"}).AddRow("kewei", 30)
	s.mockReplica[numReplica-1].ExpectQuery("^SELECT (.+) FROM table").WithArgs("kewei").WillReturnRows(returnRows)
	// Run
	rows, err := s.mydb.QueryContext(context.Background(), "SELECT * FROM table WHERE name=?", "kewei")
	defer rows.Close()
	// Check result
	var name string
	var age int
	for rows.Next() {
		s.Require().NoError(rows.Scan(&name, &age))
		s.Equal("kewei", name)
		s.Equal(30, age)
	}
	s.Require().Equal(err, nil)
}

func (s *mydbSuite) TestQueryRow() {
	// Close some replicas except for last one
	for i := 0; i < numReplica-1; i++ {
		s.replica[i].Close()
	}
	// Prepare mock result
	returnRows := sqlmock.NewRows([]string{"name", "age"}).AddRow("kewei", 30)
	s.mockReplica[numReplica-1].ExpectQuery("^SELECT (.+) FROM table").WithArgs("kewei").WillReturnRows(returnRows)
	// Run
	var name string
	var age int
	err := s.mydb.QueryRow("SELECT * FROM table WHERE name=?", "kewei").Scan(&name, &age)
	s.Require().NoError(err)
	s.Equal("kewei", name)
	s.Equal(30, age)
}

func (s *mydbSuite) TestQueryRowContext() {
	// Close some replicas except for last one
	for i := 0; i < numReplica-1; i++ {
		s.replica[i].Close()
	}
	// Prepare mock result
	returnRows := sqlmock.NewRows([]string{"name", "age"}).AddRow("kewei", 30)
	s.mockReplica[numReplica-1].ExpectQuery("^SELECT (.+) FROM table").WithArgs("kewei").WillReturnRows(returnRows)
	// Run
	var name string
	var age int
	err := s.mydb.QueryRowContext(context.Background(), "SELECT * FROM table WHERE name=?", "kewei").Scan(&name, &age)
	s.Require().NoError(err)
	s.Equal("kewei", name)
	s.Equal(30, age)
}

func (s *mydbSuite) TestExec() {
	s.mockMaster.ExpectExec("^INSERT INTO table").WithArgs("kewei", 30).WillReturnResult(sqlmock.NewResult(1, 1))
	res, err := s.mydb.Exec("INSERT INTO table (name, age) VALUES (?, ?)", "kewei", 30)
	// Check result
	s.Require().NoError(err)
	id, err := res.LastInsertId()
	s.Require().NoError(err)
	s.Equal(int64(1), id)
	rowsAffect, err := res.RowsAffected()
	s.Require().NoError(err)
	s.Equal(int64(1), rowsAffect)
}

func (s *mydbSuite) TestExecContext() {
	s.mockMaster.ExpectExec("^INSERT INTO table").WithArgs("kewei", 30).WillReturnResult(sqlmock.NewResult(1, 1))
	res, err := s.mydb.ExecContext(context.Background(), "INSERT INTO table (name, age) VALUES (?, ?)", "kewei", 30)
	// Check result
	s.Require().NoError(err)
	id, err := res.LastInsertId()
	s.Require().NoError(err)
	s.Equal(int64(1), id)
	rowsAffect, err := res.RowsAffected()
	s.Require().NoError(err)
	s.Equal(int64(1), rowsAffect)
}

func (s *mydbSuite) TestTransactionBegin() {
	// We expect the begin/exec/commit runs via master db.
	s.mockMaster.ExpectBegin()
	s.mockMaster.ExpectExec("^INSERT INTO table").WithArgs("kewei", 30).WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockMaster.ExpectCommit()
	// Run
	tx, err := s.mydb.Begin()
	s.Require().NoError(err)
	_, err = tx.Exec("INSERT INTO table (name, age) VALUES (?, ?)", "kewei", 30)
	s.Require().NoError(err)
	s.Require().NoError(tx.Commit())
}

func (s *mydbSuite) TestTransactionBeginTx() {
	// We expect the begin/exec/commit runs via master db.
	s.mockMaster.ExpectBegin()
	s.mockMaster.ExpectExec("^INSERT INTO table").WithArgs("kewei", 30).WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockMaster.ExpectCommit()
	// Run
	tx, err := s.mydb.BeginTx(context.Background(), nil)
	s.Require().NoError(err)
	_, err = tx.Exec("INSERT INTO table (name, age) VALUES (?, ?)", "kewei", 30)
	s.Require().NoError(err)
	s.Require().NoError(tx.Commit())
}

func (s *mydbSuite) TestPrepare() {
	s.mockMaster.ExpectPrepare("^INSERT INTO table").ExpectExec().WithArgs("kewei", 30).WillReturnResult(sqlmock.NewResult(1, 1))
	// Run
	stmt, err := s.mydb.Prepare("INSERT INTO table (name, age) VALUES (?, ?)")
	defer stmt.Close()
	s.Require().NoError(err)
	_, err = stmt.Exec("kewei", 30)
	s.Require().NoError(err)
}

func TestMydb(t *testing.T) {
	suite.Run(t, new(mydbSuite))
}
