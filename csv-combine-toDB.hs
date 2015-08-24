{-# OPTIONS_GHC -Wall                    #-}
--{-# OPTIONS_GHC -fno-warn-unused-binds   #-}
--{-# OPTIONS_GHC -fno-warn-unused-imports #-}

{-
Requires modules: HDBC HDBC-sqlite3 directory split
-}

import Control.Monad
import Data.List

import qualified Data.List.Split as L.Split (splitOn, wordsBy)
import System.Directory (removeFile)
import Control.Exception (catch, throwIO, bracket)
import System.IO.Error (isDoesNotExistError)
import qualified Database.HDBC.Sqlite3 as DB (connectSqlite3)
import qualified Database.HDBC as DB


-- | Removes file if exists. Error catching is used instead of explicitly
--   testing for the existence of file to eliminate race conditions. See
--   https://stackoverflow.com/a/8502391
removeIfExists :: FilePath -> IO ()
removeIfExists fileName = removeFile fileName `catch` (\e ->
    if isDoesNotExistError e
        then pure ()
        else throwIO e
    )

-- | Naive CSV decoding.
decodeCSV :: String -> [[String]]
decodeCSV = map (L.Split.splitOn ",") . L.Split.wordsBy (`elem` "\n\r")

-- | Add columns to database. No error if the columns already exist.
addColumnsIfNotExists :: (DB.IConnection c)
    => c -- ^ Connection handle
    -> String -- ^ Table name
    -> [String] -- ^ List of column names to be added
    -> IO ()
addColumnsIfNotExists conn tableName colNameS = do
    curColS <- map fst <$> DB.describeTable conn tableName
    -- Helper function that acts on each column name to be added.
    let addColumnIfNotExists newCol =
            when (newCol `notElem` curColS) $ void $
                DB.run conn ("ALTER TABLE " ++ tableName ++ " ADD COLUMN " ++ newCol) []
    mapM_ addColumnIfNotExists colNameS

-- | Add CSV record. Note pattern of UPDATE then INSERT. See
--   https://stackoverflow.com/a/15277374
addCsvRecordFromFile :: (DB.IConnection c)
    => c -- ^ Connection handle
    -> String -- ^ Table name
    -> FilePath -- ^ Path of CSV file
    -> IO ()
addCsvRecordFromFile conn tableName fileName = do
    headName:headUnits:csvData <- decodeCSV <$> readFile fileName
        -- Put units together with header name. Note that the result is escaped with quotes.
    let h1 = zipWith (\name units -> '\"' : name ++ '(' : units ++ ")\"") headName headUnits
        header :: [String]
        header = "date":"time": drop 2 h1

    addColumnsIfNotExists conn tableName header

    -- Build UPDATE statement
    let varString :: String
        varString = intercalate "=?, " (drop 2 header) ++ "=?"
    updStmt <- DB.prepare conn ("UPDATE " ++ tableName ++ " SET " ++ varString ++ " WHERE date=? AND time=?")

    -- Build INSERT OR IGNORE statement
    let headerString :: String
        headerString = '(' : intercalate "," header ++ ")"
        questionMarkString :: String -- ^ String of '?' separated by ','
        questionMarkString = '(' : intercalate "," (replicate (length header) "?") ++ ")"
    insStmt <- DB.prepare conn ("INSERT OR IGNORE INTO " ++ tableName ++ headerString ++ " VALUES " ++ questionMarkString)

    forM_ csvData (\row -> do
        let sqlAll@(sqlDate:sqlTime:sqlRest) = map DB.toSql row
        _ <- DB.execute updStmt $ sqlRest ++ [sqlDate,sqlTime]
        DB.execute insStmt sqlAll
        )


main :: IO ()
main = do
    let dbName = "output.db"
        tableName = "data"

    removeIfExists dbName

    -- Connect / create database. This is SQLite specific!
    -- `bracket` will disconnect database in event of any error.
    bracket (DB.connectSqlite3 dbName) DB.disconnect (\conn -> do
        -- Create table. WITHOUT ROWID is also SQLite specific.
        _ <- DB.run conn ("CREATE TABLE " ++ tableName ++
            " (date TEXT NOT NULL, time TEXT NOT NULL, PRIMARY KEY (date,time)) WITHOUT ROWID") []

        addCsvRecordFromFile conn tableName "data/2014 data/CHW Riser 1 _ 2/JAN.CSV"
        addCsvRecordFromFile conn tableName "data/2014 data/SYSTEM EFFICIENCY/JAN.CSV"

        DB.commit conn
        )

