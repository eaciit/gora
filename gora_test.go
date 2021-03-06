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

func TestQueryFilter(t *testing.T) {
	cv.Convey("querying", t, func() {
		conn, _ := connect()
		defer conn.Close()

		cmd := dbflex.From(tableModel).Select().Where(dbflex.And(dbflex.Gte("ID", "data-2"), dbflex.Lte("ID", "data-4")))
		cur := conn.Cursor(cmd, nil)
		cv.So(cur.Error(), cv.ShouldBeNil)

		cv.Convey("validate", func() {
			ms := []toolkit.M{}
			err := cur.Fetchs(&ms, 0)
			defer cur.Close()

			cv.So(err, cv.ShouldBeNil)
			cv.So(len(ms), cv.ShouldEqual, 3)
		})
	})
}

func TestQuerySortTake(t *testing.T) {
	cv.Convey("querying", t, func() {
		conn, _ := connect()
		defer conn.Close()

		cmd := dbflex.From(tableModel).Select().OrderBy("-ID").Take(3)
		cur := conn.Cursor(cmd, nil)
		cv.So(cur.Error(), cv.ShouldBeNil)

		cv.Convey("validate", func() {
			ms := []toolkit.M{}
			err := cur.Fetchs(&ms, 0)
			defer cur.Close()

			cv.So(err, cv.ShouldBeNil)
			cv.So(len(ms), cv.ShouldEqual, 3)

			cv.So(ms[2].GetString("ID"), cv.ShouldEqual, "data-2")
		})
	})
}

func TestQueryUpdate(t *testing.T) {
	cv.Convey("querying data", t, func() {
		c, _ := connect()
		defer c.Close()

		cmdSelect := dbflex.From(tableModel).Select().Where(dbflex.Eq("ID", "data-3"))
		cursor := c.Cursor(cmdSelect, nil)
		cv.So(cursor.Error(), cv.ShouldBeNil)
		defer cursor.Close()

		cv.Convey("update result", func() {
			results := []toolkit.M{}
			err := cursor.Fetchs(&results, 0)
			cv.So(err, cv.ShouldBeNil)
			cv.So(len(results), cv.ShouldEqual, 1)

			dataInt := toolkit.RandInt(100) + 500
			results[0]["DATAINT"] = dataInt

			cmdUpdate := dbflex.From(tableModel).Update().Where(dbflex.Eq("ID", "data-3"))
			_, err = c.Execute(cmdUpdate, toolkit.M{}.Set("data", results[0]))
			cv.So(err, cv.ShouldBeNil)

			cv.Convey("validate", func() {
				cur2 := c.Cursor(cmdSelect, nil)
				defer cur2.Close()

				results2 := []toolkit.M{}
				err = cur2.Fetchs(&results2, 0)
				cv.So(err, cv.ShouldBeNil)
				cv.So(len(results2), cv.ShouldEqual, 1)
				cv.So(results2[0].GetInt("DATAINT"), cv.ShouldEqual, dataInt)
			})
		})
	})
}

/*
INSERT INTO TestModel (ID,Title,DataInt,DataDec,Created) VALUES ('data-0','Data title 0',31,27.150000,to_date('2019-02-23 12:54:18','yyyy-mm-dd hh24:mi:ss'))
SELECT a.* FROM (SELECT tmp.* FROM (SELECT * FROM TestModel  ORDER BY ID desc ) tmp WHERE ROWNUM <  3) a WHERE ROWNUM <= 1;
*/
