{-# OPTIONS_GHC -Wall                    #-}
--{-# OPTIONS_GHC -fno-warn-unused-binds   #-}
--{-# OPTIONS_GHC -fno-warn-unused-imports #-}
{-# LANGUAGE FlexibleContexts            #-}

import Control.Monad
import Control.Monad.Reader
import Pipes
import Data.List

import qualified Data.Text.Lazy as T (Text, unpack, splitOn, split, null, singleton)
import qualified Data.Text.Lazy.IO as T (readFile)
import Data.Char (toLower)
import System.FilePath ((</>))
import System.Directory (removeFile, getDirectoryContents, doesDirectoryExist)
import Control.Exception (catch, throwIO, bracket)
import System.IO.Error (isDoesNotExistError)
import qualified Database.HDBC.Sqlite3 as DBSL (Connection, connectSqlite3)
import qualified Database.HDBC as DB


data DbTblConnection = DbTblConnection
    DBSL.Connection -- ^ Database connection. The type is database specific!
    String -- ^ Name of table in database.

-- | Removes file if exists. Error catching is used instead of explicitly
--   testing for the existence of file to eliminate race conditions. See
--   https://stackoverflow.com/a/8502391
removeFileIfExists :: FilePath -> IO ()
removeFileIfExists fileName = removeFile fileName `catch` (\e ->
    if isDoesNotExistError e
        then pure ()
        else throwIO e
    )

-- | A `Producer` that outputs filepaths of files ending in ".csv". If given a
--   directory, it will traverse the directory depth-first looking for "*.csv"
--   files.
getRecursiveCSVPath :: (MonadIO m)
                    => FilePath -> Producer FilePath m ()
getRecursiveCSVPath path = do
    -- isDirectory is checked before checking ".csv" suffix because a folder might end in ".csv"
    isDirectory <- liftIO $ doesDirectoryExist path
    if isDirectory
        then do
            names <- liftIO $ getDirectoryContents path
                -- This filters out all files beginning with ".", including "./" and "../"
            let properNames = filter ((/= '.') . head) names
                properPath = map (path </>) properNames
            for (each properPath) getRecursiveCSVPath
        else if ".csv" `isSuffixOf` map toLower path
            then yield path
            else pure ()

-- | `Pipe` that takes in a filepath for a CSV file and outputs parsed CSV data.
--   The list output is generated lazily; i.e. the input file is only read when
--   elements of the list are used.
--   Improvement: Use a more robust CSV parser.
getCsvRecordFromFile :: (MonadIO m)
                     => Pipe FilePath [[T.Text]] m ()
getCsvRecordFromFile = forever $ do
    fileName <- await
    csvData <- liftIO $ decodeCSV <$> T.readFile fileName
    yield csvData
  where
    -- | Naive CSV decoding.
    decodeCSV :: T.Text -> [[T.Text]]
    decodeCSV = map (T.splitOn $ T.singleton ',') .
                    filter (not . T.null) . T.split (`elem` "\n\r")

-- | Add columns to database. No error if the columns already exist.
--   Improvement: Should attempt to add column and then catch SQL error if it
--   already exists, for the same reasons as `removeFileIfExists`. Problem: how
--   to catch error in a robust and database portable way? Note: SQLite does not
--   support the `IF NOT EXISTS` command in `ALTER TABLE ___ ADD COLUMN ___ IF
--   NOT EXISTS`.
addColumnsIfNotExists :: (MonadReader DbTblConnection m, MonadIO m)
                      => [String] -- ^ List of column names to be added
                      -> m ()
addColumnsIfNotExists colNameS = do
    DbTblConnection conn tableName <- ask

    curColS <- liftIO $ map fst <$> DB.describeTable conn tableName
    -- Helper function that acts on each column name to be added.
    let addColumnIfNotExists newCol =
            when (newCol `notElem` curColS) $ void $
                DB.run conn ("ALTER TABLE " ++ tableName ++ " ADD COLUMN \"" ++ newCol ++ "\"") []
    liftIO $ mapM_ addColumnIfNotExists colNameS

-- | A `consumer` that adds CSV records to database. Note pattern of `UPDATE`
--   then `INSERT OR IGNORE`. See https://stackoverflow.com/a/15277374
addCsvRecordToDB :: (MonadReader DbTblConnection m, MonadIO m)
                 => Consumer [[T.Text]] m ()
addCsvRecordToDB = forever $ do
    headName:headUnits:csvData <- await
    DbTblConnection conn tableName <- ask

        -- Put units together with header name.
    let h1 :: [String]
        h1 = zipWith (\name units -> T.unpack name ++ '(' : T.unpack units ++ ")")
                headName headUnits
        header :: [String]
        header = "date":"time": drop 2 h1

    addColumnsIfNotExists header

    -- Build UPDATE statement
    let varString :: String
        varString = '\"' : (intercalate "\"=?, \"" (drop 2 header)) ++ "\"=?"
    updStmt <- liftIO $ DB.prepare conn ("UPDATE " ++ tableName ++ " SET " ++ varString ++ " WHERE date=? AND time=?")

    -- Build INSERT OR IGNORE statement
    let headerString :: String
        headerString = "(\"" ++ intercalate "\",\"" header ++ "\")"
        questionMarkString :: String -- ^ String of '?' separated by ','
        questionMarkString = '(' : intercalate "," (replicate (length header) "?") ++ ")"
    insStmt <- liftIO $ DB.prepare conn ("INSERT OR IGNORE INTO " ++ tableName ++ headerString ++ " VALUES " ++ questionMarkString)

    liftIO $ forM_ csvData (\row -> do
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

        runReaderT (runEffect $
            getRecursiveCSVPath "data/2015 data" >-> getCsvRecordFromFile >-> addCsvRecordToDB
            ) dbTable

        DB.commit conn
        )

