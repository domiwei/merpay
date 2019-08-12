package mydb

import (
	"database/sql"
	"testing"
	"time"

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
	//s.master.Close()
	for i := 0; i < numReplica; i++ {
		s.NoError(s.mockReplica[i].ExpectationsWereMet())
		//s.replica[i].Close()
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
	rows.Close()
	s.Require().Equal(err, nil)
	time.Sleep(time.Second)
}

func TestMydb(t *testing.T) {
	suite.Run(t, new(mydbSuite))
}
