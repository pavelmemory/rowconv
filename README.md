# rowconv
Compact and simple converter from 'database/sql/Row(s)' into basic types or user-defined structs

## How to test
This package is free of third party dependencies, but requires database drivers for testing.
To get drivers you can use `dep ensure` command inside working folder.
It will grab internet for necessary dependencies.


To test with Postgres database start it's container first:
```bash
SQL_PORT=32100 && \
docker run --name=rowconv \
    -p 127.0.0.1:$SQL_PORT:$SQL_PORT \
    -e POSTGRES_USER="user" \
    -e POSTGRES_PASSWORD="password" \
    -e POSTGRES_DB="dev" \
    -d postgres \
    -p $SQL_PORT
```


To test with MySQL database start it's container first:
```bash
SQL_PORT=32100 && \
docker run --name=rowconv \
    -p 127.0.0.1:$SQL_PORT:$SQL_PORT \
    -e MYSQL_DATABASE="dev" \
    -e MYSQL_USER="user" \
    -e MYSQL_PASSWORD="password" \
    -e MYSQL_ROOT_HOST="172.*.*.*" \
    -e MYSQL_ROOT_PASSWORD="root" \
    -d mysql/mysql-server \
    -P $SQL_PORT
```

After testing remove unused container with command:
```bash
docker rm -f rowconv
```