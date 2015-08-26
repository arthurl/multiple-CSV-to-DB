# CSV combine to DB

Script to crawl a folder for CSV files and upsert each one into a combined database table.

It processes CSV files in constant space and linear time (not including the time and space needed for database operations), with respect to both size and number of CSV files.

## How to use

Modify the variables `dbFileName`, `tableName` and `csvPath` defined at the start of `main` to the appropriate values.

## CSV format

Assumes that first row = header, and second row = units. CSV lines and rows are delimited with `'\n'` and `','` respectively.
