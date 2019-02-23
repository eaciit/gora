package gora_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/eaciit/toolkit"
	cv "github.com/smartystreets/goconvey/convey"

	"git.eaciitapp.com/sebar/dbflex"
)

type DataModel struct {
	ID      string
	Title   string
	DataInt int
	DataDec float64
	Created time.Time
}

var (
	connectionString = "oracle://scbdc:Password@localhost:1521/orclpdb1"
	tableTest        = "TestTable"
	tableModel       = "TestModel"
)

func connect() (dbflex.IConnection, error) {
	conn, err := dbflex.NewConnectionFromURI(connectionString, nil)
	if err != nil {
		return nil, fmt.Errorf("connection init error. %s", err.Error())
	}
	err = conn.Connect()
	if err != nil {
		return nil, fmt.Errorf("unable to connect %s. %s", connectionString, err.Error())
	}
	return conn, nil
}

func TestConnect(t *testing.T) {
	cv.Convey("connect", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()
	})
}

func TestClearTable(t *testing.T) {
	cv.Convey("delete data", t, func() {
		c, _ := connect()
		defer c.Close()

		cmd := dbflex.From(tableModel).Delete()
		_, err := c.Execute(cmd, nil)
		cv.So(err, cv.ShouldBeNil)
	})
}

var ndata = 5

func TestInsertData(t *testing.T) {
	cv.Convey("save data", t, func() {
		c, _ := connect()
		defer c.Close()

		cmd := dbflex.From(tableModel).Insert()
		q, err := c.Prepare(cmd)
		cv.So(err, cv.ShouldBeNil)

		cv.Convey("process", func() {
			es := []string{}
			for i := 0; i < ndata; i++ {
				model := new(DataModel)
				model.ID = fmt.Sprintf("data-%d", i)
				model.Title = fmt.Sprintf("Data title %d", i)
				model.DataInt = toolkit.RandInt(50)
				model.DataDec = toolkit.RandFloat(1000, 2)
				model.Created = time.Now()
				_, err := q.Execute(toolkit.M{}.Set("data", model))
				if err != nil {
					es = append(es, fmt.Sprintf("saving error %d. %s", i+1, err.Error()))
				}
			}
			esTxt := strings.Join(es, "\n")
			cv.So(esTxt, cv.ShouldEqual, "")
		})
	})
}

func TestQueryUsingModel(t *testing.T) {
	cv.Convey("querying data", t, func() {
		c, _ := connect()
		defer c.Close()

		cmd := dbflex.From(tableModel).Select()
		cursor := c.Cursor(cmd, nil)
		cv.So(cursor.Error(), cv.ShouldBeNil)
		defer cursor.Close()

		cv.Convey("validate result", func() {
			results := []DataModel{}
			err := cursor.Fetchs(&results, 0)
			cv.So(err, cv.ShouldBeNil)
			cv.So(len(results), cv.ShouldEqual, ndata)

			fmt.Println("Results:\n", toolkit.JsonString(results))
		})
	})
}

func TestQueryUsingM(t *testing.T) {
	cv.Convey("querying data", t, func() {
		c, _ := connect()
		defer c.Close()

		cmd := dbflex.From(tableModel).Select()
		cursor := c.Cursor(cmd, nil)
		cv.So(cursor.Error(), cv.ShouldBeNil)
		defer cursor.Close()

		cv.Convey("validate result", func() {
			results := []toolkit.M{}
			err := cursor.Fetchs(&results, 0)
			cv.So(err, cv.ShouldBeNil)
			cv.So(len(results), cv.ShouldEqual, ndata)

			fmt.Println("Results:\n", toolkit.JsonString(results))
		})
	})
}

/*
INSERT INTO TestModel (ID,Title,DataInt,DataDec,Created) VALUES ('data-0','Data title 0',31,27.150000,to_date('2019-02-23 12:54:18','yyyy-mm-dd hh24:mi:ss'))
*/
