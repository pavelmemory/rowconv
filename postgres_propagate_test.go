// +build postgres

package main

import (
	_ "github.com/lib/pq"
)

func driverName() string {
	return postgres
}

func dataSourceURL() string {
	return postgres+"://"+user+":"+password+"@localhost:"+port+"/"+schema+"?sslmode=disable"
}

func ddlCreateTestTempTable() string {
	return `
CREATE TEMPORARY TABLE IF NOT EXISTS propagation(
	id DECIMAL(6,0) PRIMARY KEY,
	col1 VARCHAR(20) NOT NULL,
	col2 VARCHAR(10),
	col3 TIMESTAMP WITH TIME ZONE
)`
}