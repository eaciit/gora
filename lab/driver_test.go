package lab_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/eaciit/toolkit"
	cv "github.com/smartystreets/goconvey/convey"

	_ "github.com/go-goracle/goracle"
)

var (
	connString = "scbdc/Password@localhost:1521/orclpdb1"
)

func connect() (*sql.DB, error) {
	return sql.Open("goracle", connString)
}

func TestConnect(t *testing.T) {
	cv.Convey("connect", t, func() {
		db, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer db.Close()
	})
}

func TestQuery(t *testing.T) {
	cv.Convey("connect", t, func() {
		db, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer db.Close()

		cv.Convey("querying data", func() {
			rows, err := db.Query("select * from TestTable")
			cv.So(err, cv.ShouldBeNil)
			defer rows.Close()

			fmt.Println()
			columns, _ := rows.Columns()
			columnTypes, _ := rows.ColumnTypes()
			fmt.Printf("Rows info: \ncolumns:%v\ntypes:%v\n",
				toolkit.JsonString(columns),
				columnTypes[0].ScanType().String())

			cv.Convey("validate", func() {
				iRead := 0
				fmt.Println()
				for {
					if rows.Next() {
						iRead++
						var id, title string
						scanErr := rows.Scan(&id, &title)
						cv.So(scanErr, cv.ShouldBeNil)

						if scanErr == nil {
							m := toolkit.M{}.Set("id", id).Set("name", title)
							fmt.Printf("Read data %d: %v\n", iRead, m)
						}
					} else {
						break
					}
				}
				cv.So(iRead, cv.ShouldBeGreaterThan, 0)
			})
		})
	})
}
