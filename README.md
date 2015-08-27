# Merge multiple CSVs into one SQL database

Script to crawl a folder for CSV files and [upserts](https://en.wikipedia.org/wiki/Upsert) each one into a combined database table.

It processes CSV files in constant space and linear time (not including the time and space needed for database operations), with respect to both size and number of CSV files.

## How to use

### Edit script

Modify the variables `dbFileName`, `tableName` and `csvPath` defined at the start of `main` to the appropriate values.

### Running the script

1. Make sure you have GHC and Cabal installed (GHC v 7.10.2 and Cabal v 1.22 tested).
2. Clone this repository `git clone git@github.com:arthurl/multiple-CSV-to-DB.git && cd multiple-CSV-to-DB`.
3. (Highly recommended, but not necessary.) Use cabal sandbox `cabal sandbox init`. Otherwise the package and all its dependencies will be installed system wide.
4. Install dependencies `cabal install --only-dependencies`.
5. Build & run `cabal build && cabal run`.

You don't need, nor should you, use `sudo` for any of these commands (except installing GHC and Cabal)! 

## CSV format

Assumes that first row = header, and second row = units. CSV lines and rows are delimited with `'\n'` and `','` respectively.
