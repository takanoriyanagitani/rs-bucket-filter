#!/bin/sh

export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export PGPASSWORD=postgres
export PGDATABASE=postgres
export PGAPPNAME=test-bucket-filter

./target/release/pg-get-or-skip
