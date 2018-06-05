package main

import (
	"database/sql"
	"os"
	"testing"

	"context"
	_ "github.com/go-sql-driver/mysql"
	"time"
	"reflect"
)

var db *sql.DB

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
	mySQL, err := sql.Open("mysql", "root:root@/Development?parseTime=true")
	if err != nil {
		panic(err)
	}
	db = mySQL
}

func closeDbConnection() {
	if db != nil {
		db.Close()
	}
}

func TestPropagate(t *testing.T) {
	checks := []struct {
		scenario string
		action   func(tx *sql.Tx) func(t *testing.T)
	}{
		{
			scenario: "retrieve single column and store into a slice of value type",
			action: func(tx *sql.Tx) func(t *testing.T) {
				return func(t *testing.T) {
					initCtx, initCancel := context.WithTimeout(context.Background(), time.Second)
					defer initCancel()
					_, err := tx.ExecContext(initCtx, "INSERT INTO propagation(id, col1) VALUES (1, 'a'), (2, 'b')")
					if err != nil {
						t.Fatal(err)
					}

					runCtx, runCancel := context.WithTimeout(context.Background(), time.Second)
					defer runCancel()
					rows, err := tx.QueryContext(runCtx, "SELECT id FROM propagation ORDER BY id")
					if err != nil {
						t.Fatal(err)
					}

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
			scenario: "retrieve single column and store into a slice of reference type",
			action: func(tx *sql.Tx) func(t *testing.T) {
				return func(t *testing.T) {
					initCtx, initCancel := context.WithTimeout(context.Background(), time.Second)
					defer initCancel()
					_, err := tx.ExecContext(initCtx, "INSERT INTO propagation(id, col1) VALUES (1, 'a'), (2, 'b')")
					if err != nil {
						t.Fatal(err)
					}

					runCtx, runCancel := context.WithTimeout(context.Background(), time.Second)
					defer runCancel()
					rows, err := tx.QueryContext(runCtx, "SELECT id FROM propagation ORDER BY id")
					if err != nil {
						t.Fatal(err)
					}

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
			scenario: "retrieve single column with NULL and store into a slice of reference type",
			action: func(tx *sql.Tx) func(t *testing.T) {
				return func(t *testing.T) {
					initCtx, initCancel := context.WithTimeout(context.Background(), time.Second)
					defer initCancel()
					_, err := tx.ExecContext(initCtx, "INSERT INTO propagation(id, col1, col2) VALUES (1, '', NULL), (2, '', 'b')")
					if err != nil {
						t.Fatal(err)
					}

					runCtx, runCancel := context.WithTimeout(context.Background(), time.Second)
					defer runCancel()
					rows, err := tx.QueryContext(runCtx, "SELECT col2 FROM propagation ORDER BY id")
					if err != nil {
						t.Fatal(err)
					}

					var col2s []*string
					if err := Propagate(&col2s, rows); err != nil {
						t.Fatal(err)
					}
					if col2s[0] != nil || *col2s[1] != "b" {
						t.Errorf("unexpeted results of propagation: %v", col2s)
					}
				}
			},
		}, {
			scenario: "retrieve multiple columns and store into a slice of value struct type",
			action: func(tx *sql.Tx) func(t *testing.T) {
				return func(t *testing.T) {
					initCtx, initCancel := context.WithTimeout(context.Background(), time.Second)
					defer initCancel()
					_, err := tx.ExecContext(initCtx, "INSERT INTO propagation(id, col1, col2) VALUES (1, 'a', NULL), (2, 'b', 'c')")
					if err != nil {
						t.Fatal(err)
					}

					runCtx, runCancel := context.WithTimeout(context.Background(), time.Second)
					defer runCancel()
					rows, err := tx.QueryContext(runCtx, "SELECT id, col1, col2 FROM propagation ORDER BY id")
					if err != nil {
						t.Fatal(err)
					}

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
			scenario: "retrieve multiple columns and store into a slice of reference struct type",
			action: func(tx *sql.Tx) func(t *testing.T) {
				return func(t *testing.T) {
					initCtx, initCancel := context.WithTimeout(context.Background(), time.Second)
					defer initCancel()
					_, err := tx.ExecContext(initCtx, "INSERT INTO propagation(id, col1, col2) VALUES (1, 'a', NULL), (2, 'b', 'c')")
					if err != nil {
						t.Fatal(err)
					}

					runCtx, runCancel := context.WithTimeout(context.Background(), time.Second)
					defer runCancel()
					rows, err := tx.QueryContext(runCtx, "SELECT id, col1, col2 FROM propagation ORDER BY id")
					if err != nil {
						t.Fatal(err)
					}

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
			scenario: "retrieve multiple columns and store into a slice of reference struct type with tagged column name",
			action: func(tx *sql.Tx) func(t *testing.T) {
				return func(t *testing.T) {
					initCtx, initCancel := context.WithTimeout(context.Background(), time.Second)
					defer initCancel()
					row1CreationTimestamp := time.Now().UTC().Round(time.Second)
					_, err := tx.ExecContext(initCtx, "INSERT INTO propagation(id, col1, col3) VALUES (1, 'a', '"+row1CreationTimestamp.Format("2006-01-02 15:04:05")+"'), (2, 'b', NULL)")
					if err != nil {
						t.Fatal(err)
					}

					runCtx, runCancel := context.WithTimeout(context.Background(), time.Second)
					defer runCancel()
					rows, err := tx.QueryContext(runCtx, "SELECT id, col3 as creation_time FROM propagation ORDER BY id")
					if err != nil {
						t.Fatal(err)
					}

					type refStruct struct {
						PK                int        `db_column:"id"`
						CreationTimestamp *time.Time `db_column:"creation_time"`
					}
					var refStructs []*refStruct
					if err := Propagate(&refStructs, rows); err != nil {
						t.Fatal(err)
					}
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
			scenario: "retrieve multiple columns and store into a slice of value struct type with embedded field of simple type",
			action: func(tx *sql.Tx) func(t *testing.T) {
				return func(t *testing.T) {
					initCtx, initCancel := context.WithTimeout(context.Background(), time.Second)
					defer initCancel()
					_, err := tx.ExecContext(initCtx, "INSERT INTO propagation(id, col1) VALUES (1, 'a'), (2, 'b')")
					if err != nil {
						t.Fatal(err)
					}

					runCtx, runCancel := context.WithTimeout(context.Background(), time.Second)
					defer runCancel()
					rows, err := tx.QueryContext(runCtx, "SELECT id, col1 FROM propagation ORDER BY id")
					if err != nil {
						t.Fatal(err)
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
			scenario: "retrieve multiple columns and store into a slice of value struct type with embedded field of struct value type",
			action: func(tx *sql.Tx) func(t *testing.T) {
				return func(t *testing.T) {
					initCtx, initCancel := context.WithTimeout(context.Background(), time.Second)
					defer initCancel()
					_, err := tx.ExecContext(initCtx, "INSERT INTO propagation(id, col1) VALUES (1, 'a'), (2, 'c')")
					if err != nil {
						t.Fatal(err)
					}

					runCtx, runCancel := context.WithTimeout(context.Background(), time.Second)
					defer runCancel()
					rows, err := tx.QueryContext(runCtx, "SELECT id, col1 FROM propagation ORDER BY id")
					if err != nil {
						t.Fatal(err)
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
			scenario: "retrieve multiple columns and store into a slice of reference struct type with embedded field of struct reference type",
			action: func(tx *sql.Tx) func(t *testing.T) {
				return func(t *testing.T) {
					initCtx, initCancel := context.WithTimeout(context.Background(), time.Second)
					defer initCancel()
					_, err := tx.ExecContext(initCtx, "INSERT INTO propagation(id, col1, col2) VALUES (1, 'a', 'b'), (2, 'c', NULL)")
					if err != nil {
						t.Fatal(err)
					}

					runCtx, runCancel := context.WithTimeout(context.Background(), time.Second)
					defer runCancel()
					rows, err := tx.QueryContext(runCtx, "SELECT col2, id, col1 FROM propagation ORDER BY id")
					if err != nil {
						t.Fatal(err)
					}

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
			scenario: "retrieve multiple columns and store into a slice of reference struct type with field of struct reference type",
			action: func(tx *sql.Tx) func(t *testing.T) {
				return func(t *testing.T) {
					initCtx, initCancel := context.WithTimeout(context.Background(), time.Second)
					defer initCancel()
					_, err := tx.ExecContext(initCtx, "INSERT INTO propagation(id, col1, col2) VALUES (1, 'a', 'b'), (2, 'c', NULL)")
					if err != nil {
						t.Fatal(err)
					}

					runCtx, runCancel := context.WithTimeout(context.Background(), time.Second)
					defer runCancel()
					rows, err := tx.QueryContext(runCtx, "SELECT col2, id, col1 FROM propagation ORDER BY id")
					if err != nil {
						t.Fatal(err)
					}

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
			scenario: "retrieve multiple columns and store into a slice of reference struct type with deep nesting",
			action: func(tx *sql.Tx) func(t *testing.T) {
				return func(t *testing.T) {
					initCtx, initCancel := context.WithTimeout(context.Background(), time.Second)
					defer initCancel()
					_, err := tx.ExecContext(initCtx, "INSERT INTO propagation(id, col1, col2) VALUES (1, 'a', 'b'), (2, 'c', NULL)")
					if err != nil {
						t.Fatal(err)
					}

					runCtx, runCancel := context.WithTimeout(context.Background(), time.Second)
					defer runCancel()
					rows, err := tx.QueryContext(runCtx, "SELECT col2, id, col1 FROM propagation ORDER BY id")
					if err != nil {
						t.Fatal(err)
					}

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

		_, err = tx.ExecContext(txCtx, `
CREATE TEMPORARY TABLE IF NOT EXISTS propagation(
	id DECIMAL(6,0) PRIMARY KEY,
	col1 VARCHAR(20) NOT NULL,
	col2 CHAR(10),
	col3 DATETIME
) ENGINE = InnoDB, DEFAULT CHARACTER SET = utf8mb4`)
		if err != nil {
			t.Fatal(err)
		}

		t.Run(check.scenario, check.action(tx))
		txCancel()
		txTimeout()
	}
}

func StringRef(val string) *string {
	return &val
}
