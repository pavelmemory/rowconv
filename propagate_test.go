package main

import (
	"context"
	"database/sql"
	"os"
	"reflect"
	"testing"
	"time"
)

var db *sql.DB

const (
	port     = "32100"
	user     = "user"
	password = "password"
	schema   = "dev"
	postgres = "postgres"
	mysql    = "mysql"
)

func TestMain(t *testing.M) {
	setup()
	xCode := t.Run()
	cleanup()
	os.Exit(xCode)
}

func cleanup() {
	closeDbConnection()
}

func setup() {
	initDbConnection()
}

func initDbConnection() {
	var err error
	db, err = sql.Open(driverName(), dataSourceURL())
	if err != nil {
		panic(err)
	}
	pingCtx, pingCancel := context.WithTimeout(context.Background(), time.Second)
	defer pingCancel()
	err = db.PingContext(pingCtx)
	if err != nil {
		panic(err)
	}
}

func closeDbConnection() {
	if db != nil {
		db.Close()
	}
}

func TestPropagate(t *testing.T) {
	checks := []struct {
		scenario  string
		insert    string
		retrieval string
		action    func(rows *sql.Rows) func(t *testing.T)
	}{
		{
			scenario:  "retrieve single column and store into a slice of value type",
			insert:    "INSERT INTO propagation(id, col1) VALUES (1, 'a'), (2, 'b')",
			retrieval: "SELECT id FROM propagation ORDER BY id",
			action: func(rows *sql.Rows) func(t *testing.T) {
				return func(t *testing.T) {
					var ids []int
					if err := Propagate(&ids, rows); err != nil {
						t.Fatal(err)
					}
					if ids[0] != 1 || ids[1] != 2 {
						t.Errorf("unexpeted results of propagation: %v", ids)
					}
				}
			},
		}, {
			scenario:  "retrieve single column and store into a slice of reference type",
			insert:    "INSERT INTO propagation(id, col1) VALUES (1, 'a'), (2, 'b')",
			retrieval: "SELECT id FROM propagation ORDER BY id",
			action: func(rows *sql.Rows) func(t *testing.T) {
				return func(t *testing.T) {
					var ids []*int
					if err := Propagate(&ids, rows); err != nil {
						t.Fatal(err)
					}
					if *ids[0] != 1 || *ids[1] != 2 {
						t.Errorf("unexpeted results of propagation: %v", ids)
					}
				}
			},
		}, {
			scenario:  "retrieve single column with NULL and store into a slice of reference type",
			insert:    "INSERT INTO propagation(id, col1, col2) VALUES (1, '', NULL), (2, '', 'b')",
			retrieval: "SELECT col2 FROM propagation ORDER BY id ASC ",
			action: func(rows *sql.Rows) func(t *testing.T) {
				return func(t *testing.T) {
					var col2s []*string
					if err := Propagate(&col2s, rows); err != nil {
						t.Fatal(err)
					}
					if col2s[0] != nil {
						t.Errorf("unexpeted results of propagation: %+v", col2s[0])
					}
					if *col2s[1] != "b" {
						t.Errorf("unexpeted results of propagation: %+v", *col2s[1])
					}
				}
			},
		}, {
			scenario:  "retrieve multiple columns and store into a slice of value struct type",
			insert:    "INSERT INTO propagation(id, col1, col2) VALUES (1, 'a', NULL), (2, 'b', 'c')",
			retrieval: "SELECT id, col1, col2 FROM propagation ORDER BY id",
			action: func(rows *sql.Rows) func(t *testing.T) {
				return func(t *testing.T) {
					type valStruct struct {
						Id   int
						Col1 string
						COL2 *string
					}
					var valStructs []valStruct
					if err := Propagate(&valStructs, rows); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(valStructs[0], valStruct{Id: 1, Col1: "a"}) {
						t.Errorf("unexpeted results of propagation: %v", valStructs[0])
					}
					if !reflect.DeepEqual(valStructs[1], valStruct{Id: 2, Col1: "b", COL2: StringRef("c")}) {
						t.Errorf("unexpeted results of propagation: %v", valStructs[0])
					}
				}
			},
		}, {
			scenario:  "retrieve multiple columns and store into a slice of reference struct type",
			insert:    "INSERT INTO propagation(id, col1, col2) VALUES (1, 'a', NULL), (2, 'b', 'c')",
			retrieval: "SELECT id, col1, col2 FROM propagation ORDER BY id",
			action: func(rows *sql.Rows) func(t *testing.T) {
				return func(t *testing.T) {
					type refStruct struct {
						Id   int
						Col1 string
						COL2 *string
					}
					var refStructs []*refStruct
					if err := Propagate(&refStructs, rows); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(refStructs[0], &refStruct{Id: 1, Col1: "a"}) {
						t.Errorf("unexpeted results of propagation: %v", refStructs[0])
					}
					if !reflect.DeepEqual(refStructs[1], &refStruct{Id: 2, Col1: "b", COL2: StringRef("c")}) {
						t.Errorf("unexpeted results of propagation: %v", refStructs[0])
					}
				}
			},
		}, {
			scenario:  "retrieve multiple columns and store into a slice of reference struct type with tagged column name",
			insert:    "INSERT INTO propagation(id, col1, col3) VALUES (1, 'a', '" + time.Date(2018, time.July, 18, 13, 59, 59, 0, time.UTC).Format("2006-01-02 15:04:05") + "'), (2, 'b', NULL)",
			retrieval: "SELECT id, col3 as creation_time FROM propagation ORDER BY id",
			action: func(rows *sql.Rows) func(t *testing.T) {
				return func(t *testing.T) {
					type refStruct struct {
						PK                int        `db_column:"id"`
						CreationTimestamp *time.Time `db_column:"creation_time"`
					}
					var refStructs []*refStruct
					if err := Propagate(&refStructs, rows); err != nil {
						t.Fatal(err)
					}
					row1CreationTimestamp := time.Date(2018, time.July, 18, 13, 59, 59, 0, time.UTC)
					exp1 := &refStruct{PK: 1, CreationTimestamp: &row1CreationTimestamp}
					if !reflect.DeepEqual(refStructs[0], exp1) {
						t.Errorf("unexpeted results of propagation: expected %+v, actual %+v", exp1, refStructs[0])
					}
					exp2 := &refStruct{PK: 2}
					if !reflect.DeepEqual(refStructs[1], exp2) {
						t.Errorf("unexpeted results of propagation: expected %+v, actual %+v", exp2, refStructs[1])
					}
				}
			},
		}, {
			scenario:  "retrieve multiple columns and store into a slice of value struct type with embedded field of simple type",
			insert:    "INSERT INTO propagation(id, col1) VALUES (1, 'a'), (2, 'b')",
			retrieval: "SELECT id, col1 FROM propagation ORDER BY id",
			action: func(rows *sql.Rows) func(t *testing.T) {
				return func(t *testing.T) {
					if driverName() == postgres {
						t.Skip("postgres driver doesn't support `type Col1 []byte` types as embedded fields: " +
							"sql: Scan error on column index 1: unsupported Scan, storing driver.Value type string into type *main.Col1")
					}
					type Col1 []byte
					type valStruct struct {
						Id int
						Col1
					}
					var valStructs []valStruct
					if err := Propagate(&valStructs, rows); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(valStructs[0], valStruct{Id: 1, Col1: Col1("a")}) {
						t.Errorf("unexpeted results of propagation: %v", valStructs[0])
					}
					if !reflect.DeepEqual(valStructs[1], valStruct{Id: 2, Col1: Col1("b")}) {
						t.Errorf("unexpeted results of propagation: %v", valStructs[0])
					}
				}
			},
		}, {
			scenario:  "retrieve multiple columns and store into a slice of value struct type with embedded field of struct value type",
			insert:    "INSERT INTO propagation(id, col1) VALUES (1, 'a'), (2, 'c')",
			retrieval: "SELECT id, col1 FROM propagation ORDER BY id",
			action: func(rows *sql.Rows) func(t *testing.T) {
				return func(t *testing.T) {
					if driverName() == postgres {
						t.Skip("postgres driver doesn't support `type Col1 []byte` types as embedded fields: " +
							"sql: Scan error on column index 1: unsupported Scan, storing driver.Value type string into type *main.Col1")
					}
					type Col1 []byte
					type valStruct struct {
						Id int
						Col1
					}
					var valStructs []valStruct
					if err := Propagate(&valStructs, rows); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(valStructs[0], valStruct{Id: 1, Col1: Col1("a")}) {
						t.Errorf("unexpeted results of propagation: %v", valStructs[0])
					}
					if !reflect.DeepEqual(valStructs[1], valStruct{Id: 2, Col1: Col1("c")}) {
						t.Errorf("unexpeted results of propagation: %v", valStructs[1])
					}
				}
			},
		}, {
			scenario:  "retrieve multiple columns and store into a slice of reference struct type with embedded field of struct reference type",
			insert:    "INSERT INTO propagation(id, col1, col2) VALUES (1, 'a', 'b'), (2, 'c', NULL)",
			retrieval: "SELECT col2, id, col1 FROM propagation ORDER BY id",
			action: func(rows *sql.Rows) func(t *testing.T) {
				return func(t *testing.T) {
					type Columns struct {
						Col1 string
						Col2 *string
					}
					type refStruct struct {
						*Columns
						Id int
					}
					var refStructs []*refStruct
					if err := Propagate(&refStructs, rows); err != nil {
						t.Fatal(err)
					}
					act1 := refStructs[0]
					exp1 := &refStruct{Id: 1, Columns: &Columns{Col1: "a", Col2: StringRef("b")}}
					if !reflect.DeepEqual(act1, exp1) {
						t.Errorf("unexpeted results of propagation: expected %+v, actual %+v", exp1, act1)
					}
					act2 := refStructs[1]
					exp2 := &refStruct{Id: 2, Columns: &Columns{Col1: "c"}}
					if !reflect.DeepEqual(act2, exp2) {
						t.Errorf("unexpeted results of propagation: expected %+v, actual %+v", exp2, act2)
					}
				}
			},
		}, {
			scenario:  "retrieve multiple columns and store into a slice of reference struct type with field of struct reference type",
			insert:    "INSERT INTO propagation(id, col1, col2) VALUES (1, 'a', 'b'), (2, 'c', NULL)",
			retrieval: "SELECT col2, id, col1 FROM propagation ORDER BY id",
			action: func(rows *sql.Rows) func(t *testing.T) {
				return func(t *testing.T) {
					type columns struct {
						Col1 string
						Col2 *string
					}
					type refStruct struct {
						ComplexField *columns
						Id           int
					}
					var refStructs []*refStruct
					if err := Propagate(&refStructs, rows); err != nil {
						t.Fatal(err)
					}
					act1 := refStructs[0]
					exp1 := &refStruct{Id: 1, ComplexField: &columns{Col1: "a", Col2: StringRef("b")}}
					if !reflect.DeepEqual(act1, exp1) {
						t.Errorf("unexpeted results of propagation: expected %+v, actual %+v", exp1, act1)
					}
					act2 := refStructs[1]
					exp2 := &refStruct{Id: 2, ComplexField: &columns{Col1: "c"}}
					if !reflect.DeepEqual(act2, exp2) {
						t.Errorf("unexpeted results of propagation: expected %+v, actual %+v", exp2, act2)
					}
				}
			},
		}, {
			scenario:  "retrieve multiple columns and store into a slice of reference struct type with deep nesting",
			insert:    "INSERT INTO propagation(id, col1, col2) VALUES (1, 'a', 'b'), (2, 'c', NULL)",
			retrieval: "SELECT col2, id, col1 FROM propagation ORDER BY id",
			action: func(rows *sql.Rows) func(t *testing.T) {
				return func(t *testing.T) {
					type level1 struct {
						Id int
					}
					type level2 struct {
						Col1 string
						With *level1
					}
					type level3 struct {
						Col2 *string
						With *level2
					}
					type refStruct struct {
						With *level3
					}
					var refStructs []*refStruct
					if err := Propagate(&refStructs, rows); err != nil {
						t.Fatal(err)
					}
					act1 := refStructs[0]
					exp1 := &refStruct{With: &level3{Col2: StringRef("b"), With: &level2{Col1: "a", With: &level1{Id: 1}}}}
					if !reflect.DeepEqual(act1, exp1) {
						t.Errorf("unexpeted results of propagation: expected %+v, actual %+v", exp1, act1)
					}
					act2 := refStructs[1]
					exp2 := &refStruct{With: &level3{With: &level2{Col1: "c", With: &level1{Id: 2}}}}
					if !reflect.DeepEqual(act2, exp2) {
						t.Errorf("unexpeted results of propagation: expected %+v, actual %+v", exp2, act2)
					}
				}
			},
		},
		/*
			- check configuration of flags
			- check error cases
			- extend with map[A]B receiver
		*/
	}

	for _, check := range checks {
		txCtx, txCancel := context.WithCancel(context.Background())
		txCtx, txTimeout := context.WithTimeout(txCtx, time.Second)
		tx, err := db.BeginTx(txCtx, nil)
		if err != nil {
			t.Fatal(err)
		}

		_, err = tx.ExecContext(txCtx, ddlCreateTestTempTable())
		if err != nil {
			t.Fatal(err)
		}

		initCtx, initCancel := context.WithTimeout(context.Background(), time.Second)
		_, err = tx.ExecContext(initCtx, check.insert)
		if err != nil {
			initCancel()
			t.Fatal(err)
		}

		runCtx, runCancel := context.WithTimeout(context.Background(), time.Second)
		rows, err := tx.QueryContext(runCtx, check.retrieval)
		if err != nil {
			initCancel()
			runCancel()
			t.Fatal(err)
		}

		t.Run(check.scenario, check.action(rows))
		initCancel()
		runCancel()
		txCancel()
		txTimeout()
	}
}

// StrictColumnTypeCheck

func StringRef(val string) *string {
	return &val
}
