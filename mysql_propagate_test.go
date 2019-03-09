// +build mysql

package rowconv

import (
	_ "github.com/go-sql-driver/mysql"
)

const (
	mysql = "mysql"
)

func driverName() string {
	return mysql
}

func dataSourceURL() string {
	return user + ":" + password + "@tcp(127.0.0.1:" + port + ")/" + schema + "?parseTime=true"
}

func ddlCreateTestTempTable() string {
	return `
CREATE TEMPORARY TABLE IF NOT EXISTS propagation(
	id DECIMAL(6,0) PRIMARY KEY,
	col1 VARCHAR(20) NOT NULL,
	col2 VARCHAR(10),
	col3 DATETIME
) ENGINE = InnoDB, DEFAULT CHARACTER SET = utf8mb4`
}
