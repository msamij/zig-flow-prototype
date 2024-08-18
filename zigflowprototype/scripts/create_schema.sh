#!/bin/bash

DB_USER="root"
DB_NAME="zigflow_prototype"
SQL_FILE="./sql/schema.sql"

mysql -u $DB_USER -p $DB_NAME < $SQL_FILE