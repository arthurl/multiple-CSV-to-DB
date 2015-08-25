{-# OPTIONS_GHC -Wall                    #-}
--{-# OPTIONS_GHC -fno-warn-unused-binds   #-}
--{-# OPTIONS_GHC -fno-warn-unused-imports #-}

{-
Requires modules: transformers HDBC HDBC-sqlite3 directory text
-}

import Control.Monad
import Data.List
import Control.Monad.Trans.Class
import qualified Control.Monad.Trans.Reader as R

import qualified Data.Text.Lazy as T (Text, unpack, splitOn, split, null, singleton)
import qualified Data.Text.Lazy.IO as T (readFile)
import System.Directory (removeFile)
import Control.Exception (catch, throwIO, bracket)
import System.IO.Error (isDoesNotExistError)
import qualified Database.HDBC.Sqlite3 as DBSL (Connection, connectSqlite3)
import qualified Database.HDBC as DB


data DbTblConnection = DbTblConnection {
    getConn :: DBSL.Connection -- ^ Database connection. The type is database specific!
,   getTable :: String -- ^ Name of table in database.
}

-- | Removes file if exists. Error catching is used instead of explicitly
--   testing for the existence of file to eliminate race conditions. See
--   https://stackoverflow.com/a/8502391
removeFileIfExists :: FilePath -> IO ()
removeFileIfExists fileName = removeFile fileName `catch` (\e ->
    if isDoesNotExistError e
        then pure ()
        else throwIO e
    )

-- | Naive CSV decoding.
decodeCSV :: T.Text -> [[T.Text]]
decodeCSV = map (T.splitOn $ T.singleton ',') .
                filter (not . T.null) . T.split (`elem` "\n\r")

-- | Add columns to database. No error if the columns already exist.
addColumnsIfNotExists :: [String] -- ^ List of column names to be added
                      -> R.ReaderT DbTblConnection IO ()
addColumnsIfNotExists colNameS = do
    DbTblConnection conn tableName <- R.ask
    lift $ do
        curColS <- map fst <$> DB.describeTable conn tableName
        -- Helper function that acts on each column name to be added.
        let addColumnIfNotExists newCol =
                when (newCol `notElem` curColS) $ void $
                    DB.run conn ("ALTER TABLE " ++ tableName ++ " ADD COLUMN \"" ++ newCol ++ "\"") []
        mapM_ addColumnIfNotExists colNameS

-- | Add CSV record. Note pattern of UPDATE then INSERT. See
--   https://stackoverflow.com/a/15277374
addCsvRecordFromFile :: FilePath -- ^ Path of CSV file
                     -> R.ReaderT DbTblConnection IO ()
addCsvRecordFromFile fileName = do
    DbTblConnection conn tableName <- R.ask

    headName:headUnits:csvData <- lift $ decodeCSV <$> T.readFile fileName
        -- Put units together with header name.
    let h1 :: [String]
        h1 = zipWith (\name units -> T.unpack name ++ '(' : T.unpack units ++ ")")
                headName headUnits
        header :: [String]
        header = "date":"time": drop 2 h1

    addColumnsIfNotExists header

    lift $ do
        -- Build UPDATE statement
        let varString :: String
            varString = '\"' : (intercalate "\"=?, \"" (drop 2 header)) ++ "\"=?"
        updStmt <- DB.prepare conn ("UPDATE " ++ tableName ++ " SET " ++ varString ++ " WHERE date=? AND time=?")

        -- Build INSERT OR IGNORE statement
        let headerString :: String
            headerString = "(\"" ++ intercalate "\",\"" header ++ "\")"
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

    removeFileIfExists dbName

    -- Connect / create database. This is SQLite specific!
    -- `bracket` will disconnect database in event of any error.
    bracket (DBSL.connectSqlite3 dbName) DB.disconnect (\conn -> do
        -- Create table. WITHOUT ROWID is also SQLite specific.
        _ <- DB.run conn ("CREATE TABLE " ++ tableName ++
            " (date TEXT NOT NULL, time TEXT NOT NULL, PRIMARY KEY (date,time)) WITHOUT ROWID") []
        let dbTable = DbTblConnection conn tableName

        R.runReaderT (addCsvRecordFromFile "data/2014 data/CHW Riser 1 _ 2/JAN.CSV") dbTable
        R.runReaderT (addCsvRecordFromFile "data/2014 data/SYSTEM EFFICIENCY/JAN.CSV") dbTable

        DB.commit conn
        )

