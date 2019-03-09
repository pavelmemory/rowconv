// +build postgres

package rowconv

import (
	_ "github.com/lib/pq"
)

const (
	postgres = "postgres"
)

func driverName() string {
	return postgres
}

func dataSourceURL() string {
	return postgres + "://" + user + ":" + password + "@127.0.0.1:" + port + "/" + schema + "?sslmode=disable"
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
