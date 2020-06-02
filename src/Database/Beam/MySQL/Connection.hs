{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Database.Beam.MySQL.Connection
  ( MySQL (..),
    Connection,
    MySQLM (..),
    runBeamMySQL,
    runBeamMySQLDebug,
    runInsertRowReturning,
    MysqlCommandSyntax (..),
    MysqlSelectSyntax (..),
    MysqlInsertSyntax (..),
    MysqlUpdateSyntax (..),
    MysqlDeleteSyntax (..),
    MysqlExpressionSyntax (..),
    connect,
    close,
    mysqlUriSyntax,
  )
where

import Control.Exception
  ( Exception,
    bracket,
    displayException,
    throwIO,
  )
import Control.Monad (MonadFail (fail), forM_, when)
import Control.Monad.Free.Church (runF)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader (ReaderT (ReaderT), runReaderT)
import qualified Data.Aeson as A (Value)
import Data.ByteString.Builder (toLazyByteString)
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as BL
import Data.Functor.Identity (Identity)
import Data.Int (Int16, Int32, Int64, Int8)
import Data.List (intersect, stripPrefix)
import Data.Maybe (fromMaybe)
import Data.Ratio (Ratio)
import Data.Scientific (Scientific)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Encoding.Error as TE
import qualified Data.Text.Lazy as TL
import Data.Time (Day, LocalTime, NominalDiffTime, TimeOfDay)
import Data.Word (Word16, Word32, Word64, Word8)
import Database.Beam.Backend
  ( BeamBackend (BackendFromField),
    BeamRowReadError (BeamRowReadError),
    BeamSqlBackend,
    BeamSqlBackendIsString,
    BeamSqlBackendSyntax,
    ColumnParseError (ColumnNotEnoughColumns),
    FromBackendRow,
    FromBackendRowF
      ( Alt,
        FailParseWith,
        ParseOneField
      ),
    FromBackendRowM (FromBackendRowM),
    MonadBeam,
    SqlNull,
    defaultE,
    fromBackendRow,
    runNoReturn,
    runReturningList,
    runReturningMany,
    runReturningOne,
  )
import qualified Database.Beam.Backend.SQL.BeamExtensions as Beam
import Database.Beam.Backend.URI (BeamURIOpeners, mkUriOpener)
import Database.Beam.MySQL.FromField
  ( FromField,
    fromField,
    runFieldParser,
  )
import Database.Beam.MySQL.Syntax
  ( MysqlCommandSyntax (MysqlCommandSyntax),
    MysqlDeleteSyntax (MysqlDeleteSyntax),
    MysqlExpressionSyntax (MysqlExpressionSyntax),
    MysqlInsertSyntax (MysqlInsertSyntax),
    MysqlInsertValuesSyntax
      ( MysqlInsertSelectSyntax,
        MysqlInsertValuesSyntax
      ),
    MysqlSelectSyntax (MysqlSelectSyntax),
    MysqlSyntax (MysqlSyntax),
    MysqlTableNameSyntax (MysqlTableNameSyntax),
    MysqlUpdateSyntax (MysqlUpdateSyntax),
    emit,
    fromMysqlExpression,
    fromMysqlInsert,
    fromMysqlTableName,
    mysqlIdentifier,
    mysqlSepBy,
    unwrapInnerBuilder,
  )
import Database.Beam.Query
  ( HasQBuilder,
    HasSqlEqualityCheck,
    HasSqlQuantifiedEqualityCheck,
    SqlInsert (SqlInsert, SqlInsertNoRows),
    buildSqlQuery,
  )
import Database.Beam.Query.SQL92 (buildSql92Query')
import Database.MySQL.Base
  ( ConnectInfo (ConnectInfo),
    Connection,
    Option
      ( CharsetDir,
        CharsetName,
        Compress,
        ConnectTimeout,
        FoundRows,
        IgnoreSIGPIPE,
        IgnoreSpace,
        InitCommand,
        Interactive,
        LocalFiles,
        LocalInFile,
        MultiResults,
        MultiStatements,
        NamedPipe,
        NoSchema,
        Protocol,
        ReadDefaultFile,
        ReadDefaultGroup,
        ReadTimeout,
        Reconnect,
        ReportDataTruncation,
        SecureAuth,
        SharedMemoryBaseName,
        WriteTimeout
      ),
    Protocol (Memory, Pipe, Socket, TCP),
    close,
    connect,
    connectDatabase,
    connectHost,
    connectOptions,
    connectPassword,
    connectPath,
    connectPort,
    connectSSL,
    connectUser,
    escape,
    fetchFields,
    fetchRow,
    freeResult,
    query,
    useResult,
  )
import Database.MySQL.Base.Types (Field)
import Network.URI
  ( uriAuthority,
    uriPath,
    uriPort,
    uriQuery,
    uriRegName,
    uriUserInfo,
  )
import Text.Read (readMaybe)
import Prelude hiding (fail)

data MySQL = MySQL

instance BeamSqlBackendIsString MySQL String

instance BeamSqlBackendIsString MySQL T.Text

instance BeamBackend MySQL where
  type BackendFromField MySQL = FromField

instance BeamSqlBackend MySQL

type instance BeamSqlBackendSyntax MySQL = MysqlCommandSyntax

newtype MySQLM a = MySQLM (ReaderT (String -> IO (), Connection) IO a)
  deriving (Monad, MonadIO, Applicative, Functor)

instance MonadFail MySQLM where
  fail e = fail $ "Internal Error with: " <> show e

newtype NotEnoughColumns = NotEnoughColumns
  { _errColCount :: Int
  }
  deriving (Show)

instance Exception NotEnoughColumns where
  displayException (NotEnoughColumns colCnt) =
    mconcat
      [ "Not enough columns while reading MySQL row. Only have ",
        show colCnt,
        " column(s)"
      ]

data CouldNotReadColumn = CouldNotReadColumn
  { _errColIndex :: Int,
    _errColMsg :: String
  }
  deriving (Show)

instance Exception CouldNotReadColumn where
  displayException (CouldNotReadColumn idx msg) =
    mconcat ["Could not read column ", show idx, ": ", msg]

runBeamMySQLDebug :: (String -> IO ()) -> Connection -> MySQLM a -> IO a
runBeamMySQLDebug = withMySQL

runBeamMySQL :: Connection -> MySQLM a -> IO a
runBeamMySQL = runBeamMySQLDebug (\_ -> pure ())

instance MonadBeam MySQL MySQLM where
  runReturningMany
    (MysqlCommandSyntax (MysqlSyntax cmd))
    (consume :: MySQLM (Maybe x) -> MySQLM a) =
      MySQLM . ReaderT $ \(dbg, conn) -> do
        cmdBuilder <- cmd (\_ b _ -> pure b) (escape conn) mempty conn
        let cmdStr = BL.toStrict (toLazyByteString cmdBuilder)
        dbg (T.unpack (TE.decodeUtf8With TE.lenientDecode cmdStr))
        query conn cmdStr
        bracket (useResult conn) freeResult $ \res -> do
          fieldDescs <- fetchFields res
          let fetchRow' :: MySQLM (Maybe x)
              fetchRow' =
                MySQLM . ReaderT $ \_ -> do
                  fields <- fetchRow res
                  case fields of
                    [] -> pure Nothing
                    _ -> do
                      let FromBackendRowM go = fromBackendRow
                      rowRes <-
                        runF
                          go
                          (\x _ _ -> pure (Right x))
                          step
                          0
                          (zip fieldDescs fields)
                      case rowRes of
                        Left err -> throwIO err
                        Right x -> pure (Just x)
              parseField ::
                forall field.
                FromField field =>
                Field ->
                Maybe BS.ByteString ->
                IO (Either ColumnParseError field)
              parseField ty d = runFieldParser (fromField ty d)
              step ::
                forall y.
                FromBackendRowF MySQL (Int -> [(Field, Maybe BS.ByteString)] -> IO (Either BeamRowReadError y)) ->
                Int ->
                [(Field, Maybe BS.ByteString)] ->
                IO (Either BeamRowReadError y)
              step (ParseOneField _) curCol [] =
                pure (Left (BeamRowReadError (Just curCol) (ColumnNotEnoughColumns curCol)))
              step (ParseOneField next) curCol ((desc, field) : fields) =
                do
                  d <- parseField desc field
                  case d of
                    Left e -> pure (Left (BeamRowReadError (Just curCol) e))
                    Right d' -> next d' (curCol + 1) fields
              step (Alt (FromBackendRowM a) (FromBackendRowM b) next) curCol cols =
                do
                  aRes <- runF a (\x curCol' cols' -> pure (Right (next x curCol' cols'))) step curCol cols
                  case aRes of
                    Right next' -> next'
                    Left aErr -> do
                      bRes <- runF b (\x curCol' cols' -> pure (Right (next x curCol' cols'))) step curCol cols
                      case bRes of
                        Right next' -> next'
                        Left _ -> pure (Left aErr)
              step (FailParseWith err) _ _ = pure (Left err)
              MySQLM doConsume = consume fetchRow'
          runReaderT doConsume (dbg, conn)

withMySQL ::
  (String -> IO ()) ->
  Connection ->
  MySQLM a ->
  IO a
withMySQL dbg conn (MySQLM a) =
  runReaderT a (dbg, conn)

mysqlUriSyntax ::
  c MySQL Connection MySQLM ->
  BeamURIOpeners c
mysqlUriSyntax =
  mkUriOpener
    (withMySQL (const (pure ())))
    "mysql:"
    ( \uri ->
        let stripSuffix s a =
              reverse <$> stripPrefix (reverse s) (reverse a)
            (user, pw) =
              fromMaybe ("root", "") $ do
                userInfo <- fmap uriUserInfo (uriAuthority uri)
                userInfo' <- stripSuffix "@" userInfo
                let (user', pw') = break (== ':') userInfo'
                    pw'' = fromMaybe "" (stripPrefix ":" pw')
                pure (user', pw'')
            host =
              maybe "localhost" uriRegName . uriAuthority $ uri
            port =
              fromMaybe 3306 $ do
                portStr <- fmap uriPort (uriAuthority uri)
                portStr' <- stripPrefix ":" portStr
                readMaybe portStr'
            db =
              fromMaybe "test" $
                stripPrefix "/" (uriPath uri)
            options =
              fromMaybe [CharsetName "utf-8"] $ do
                opts <- stripPrefix "?" (uriQuery uri)
                let getKeyValuePairs "" a = a []
                    getKeyValuePairs d a =
                      let (keyValue, d') = break (== '&') d
                          attr = parseKeyValue keyValue
                       in getKeyValuePairs d' (a . maybe id (:) attr)

                pure (getKeyValuePairs opts id)
            parseBool (Just "true") = pure True
            parseBool (Just "false") = pure False
            parseBool _ = Nothing
            parseKeyValue kv = do
              let (key, value) = break (== ':') kv
                  value' = stripPrefix ":" value

              case (key, value') of
                ("connectTimeout", Just secs) ->
                  ConnectTimeout <$> readMaybe secs
                ("compress", _) -> pure Compress
                ("namedPipe", _) -> pure NamedPipe
                ("initCommand", Just cmd) ->
                  pure (InitCommand (BS.pack cmd))
                ("readDefaultFile", Just fp) ->
                  pure (ReadDefaultFile fp)
                ("readDefaultGroup", Just grp) ->
                  pure (ReadDefaultGroup (BS.pack grp))
                ("charsetDir", Just fp) ->
                  pure (CharsetDir fp)
                ("charsetName", Just nm) ->
                  pure (CharsetName nm)
                ("localInFile", b) ->
                  LocalInFile <$> parseBool b
                ("protocol", Just p) ->
                  case p of
                    "tcp" -> pure (Protocol TCP)
                    "socket" -> pure (Protocol Socket)
                    "pipe" -> pure (Protocol Pipe)
                    "memory" -> pure (Protocol Memory)
                    _ -> Nothing
                ("sharedMemoryBaseName", Just fp) ->
                  pure (SharedMemoryBaseName (BS.pack fp))
                ("readTimeout", Just secs) ->
                  ReadTimeout <$> readMaybe secs
                ("writeTimeout", Just secs) ->
                  WriteTimeout <$> readMaybe secs
                -- ( "useRemoteConnection", _ ) -> pure UseRemoteConnection
                -- ( "useEmbeddedConnection", _ ) -> pure UseEmbeddedConnection
                -- ( "guessConnection", _ ) -> pure GuessConnection
                -- ( "clientIp", Just fp) -> pure (ClientIP (BS.pack fp))
                ("secureAuth", b) ->
                  SecureAuth <$> parseBool b
                ("reportDataTruncation", b) ->
                  ReportDataTruncation <$> parseBool b
                ("reconnect", b) ->
                  Reconnect <$> parseBool b
                -- ( "sslVerifyServerCert", b) -> SSLVerifyServerCert <$> parseBool b
                ("foundRows", _) -> pure FoundRows
                ("ignoreSIGPIPE", _) -> pure IgnoreSIGPIPE
                ("ignoreSpace", _) -> pure IgnoreSpace
                ("interactive", _) -> pure Interactive
                ("localFiles", _) -> pure LocalFiles
                ("multiResults", _) -> pure MultiResults
                ("multiStatements", _) -> pure MultiStatements
                ("noSchema", _) -> pure NoSchema
                _ -> Nothing
            connInfo =
              ConnectInfo
                { connectHost = host,
                  connectPort = port,
                  connectUser = user,
                  connectPassword = pw,
                  connectDatabase = db,
                  connectOptions = options,
                  connectPath = "",
                  connectSSL = Nothing
                }
         in connect connInfo >>= \hdl -> pure (hdl, close hdl)
    )

instance FromBackendRow MySQL Bool

instance FromBackendRow MySQL Word

instance FromBackendRow MySQL Word8

instance FromBackendRow MySQL Word16

instance FromBackendRow MySQL Word32

instance FromBackendRow MySQL Word64

instance FromBackendRow MySQL Int

instance FromBackendRow MySQL Int8

instance FromBackendRow MySQL Int16

instance FromBackendRow MySQL Int32

instance FromBackendRow MySQL Int64

instance FromBackendRow MySQL Float

instance FromBackendRow MySQL Double

instance FromBackendRow MySQL Scientific

instance FromBackendRow MySQL (Ratio Integer)

instance FromBackendRow MySQL BS.ByteString

instance FromBackendRow MySQL BL.ByteString

instance FromBackendRow MySQL T.Text

instance FromBackendRow MySQL TL.Text

instance FromBackendRow MySQL Day

instance FromBackendRow MySQL LocalTime

instance FromBackendRow MySQL A.Value

instance FromBackendRow MySQL SqlNull

instance HasSqlEqualityCheck MySQL Bool

instance HasSqlQuantifiedEqualityCheck MySQL Bool

instance HasSqlEqualityCheck MySQL Double

instance HasSqlQuantifiedEqualityCheck MySQL Double

instance HasSqlEqualityCheck MySQL Float

instance HasSqlQuantifiedEqualityCheck MySQL Float

instance HasSqlEqualityCheck MySQL Int

instance HasSqlQuantifiedEqualityCheck MySQL Int

instance HasSqlEqualityCheck MySQL Int8

instance HasSqlQuantifiedEqualityCheck MySQL Int8

instance HasSqlEqualityCheck MySQL Int16

instance HasSqlQuantifiedEqualityCheck MySQL Int16

instance HasSqlEqualityCheck MySQL Int32

instance HasSqlQuantifiedEqualityCheck MySQL Int32

instance HasSqlEqualityCheck MySQL Int64

instance HasSqlQuantifiedEqualityCheck MySQL Int64

instance HasSqlEqualityCheck MySQL Word

instance HasSqlQuantifiedEqualityCheck MySQL Word

instance HasSqlEqualityCheck MySQL Word8

instance HasSqlQuantifiedEqualityCheck MySQL Word8

instance HasSqlEqualityCheck MySQL Word16

instance HasSqlQuantifiedEqualityCheck MySQL Word16

instance HasSqlEqualityCheck MySQL Word32

instance HasSqlQuantifiedEqualityCheck MySQL Word32

instance HasSqlEqualityCheck MySQL Word64

instance HasSqlQuantifiedEqualityCheck MySQL Word64

instance HasSqlEqualityCheck MySQL Integer

instance HasSqlQuantifiedEqualityCheck MySQL Integer

instance HasSqlEqualityCheck MySQL T.Text

instance HasSqlQuantifiedEqualityCheck MySQL T.Text

instance HasSqlEqualityCheck MySQL TL.Text

instance HasSqlQuantifiedEqualityCheck MySQL TL.Text

instance HasSqlEqualityCheck MySQL String

instance HasSqlQuantifiedEqualityCheck MySQL String

instance HasSqlEqualityCheck MySQL Scientific

instance HasSqlQuantifiedEqualityCheck MySQL Scientific

instance HasSqlEqualityCheck MySQL Day

instance HasSqlQuantifiedEqualityCheck MySQL Day

instance HasSqlEqualityCheck MySQL TimeOfDay

instance HasSqlQuantifiedEqualityCheck MySQL TimeOfDay

instance HasSqlEqualityCheck MySQL NominalDiffTime

instance HasSqlQuantifiedEqualityCheck MySQL NominalDiffTime

instance HasSqlEqualityCheck MySQL LocalTime

instance HasSqlQuantifiedEqualityCheck MySQL LocalTime

instance HasQBuilder MySQL where
  buildSqlQuery = buildSql92Query' True

-- https://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_last-insert-id
--
-- The ID that was generated is maintained in the server on a per-connection basis.
-- This means that the value returned by the function to a given client is the first AUTO_INCREMENT value generated
--   for most recent statement affecting an AUTO_INCREMENT column by that client.
-- This value cannot be affected by other clients,
--   even if they generate AUTO_INCREMENT values of their own.
-- This behavior ensures that each client can retrieve its own ID without concern for the activity of other clients,
--   and without the need for locks or transactions.

-- https://dev.mysql.com/doc/refman/8.0/en/create-temporary-table.html
--
-- A TEMPORARY table is visible only within the current session, and is dropped automatically when the session is closed.
-- This means that two different sessions can use the same temporary table name without conflicting with each other
--   or with an existing non-TEMPORARY table of the same name.
-- (The existing table is hidden until the temporary table is dropped.)

runInsertReturningList ::
  FromBackendRow MySQL (table Identity) =>
  SqlInsert MySQL table ->
  MySQLM [table Identity]
runInsertReturningList SqlInsertNoRows = pure []
runInsertReturningList (SqlInsert _ is@(MysqlInsertSyntax tn@(MysqlTableNameSyntax shema table) fields values)) =
  case values of
    MysqlInsertSelectSyntax _ -> fail "Not implemented runInsertReturningList part handling: INSERT INTO .. SELECT .."
    MysqlInsertValuesSyntax vals -> do
      let tableB = emit . TE.encodeUtf8Builder $ table
      let schemaB = emit $ TE.encodeUtf8Builder $ maybe "DATABASE()" (\s -> "'" <> s <> "'") shema

      (keycols :: [T.Text]) <-
        runReturningList $ MysqlCommandSyntax $
          emit "SELECT `column_name` FROM `information_schema`.`columns` WHERE "
            <> emit "`table_schema`="
            <> schemaB
            <> emit " AND `table_name`='"
            <> tableB
            <> emit "' AND `column_key` LIKE 'PRI'"

      let pk = keycols `intersect` fields

      when (null pk) $ fail "Table PK is not part of beam-table. Tables with no PK not allowed."

      (aicol :: Maybe T.Text) <-
        runReturningOne $ MysqlCommandSyntax $
          emit "SELECT `column_name` FROM `information_schema`.`columns` WHERE "
            <> emit "`table_schema`="
            <> schemaB
            <> emit " AND `table_name`='"
            <> tableB
            <> emit "' AND `extra` LIKE 'auto_increment'"

      let equalTo :: (T.Text, MysqlExpressionSyntax) -> MysqlSyntax
          equalTo (f, v) = mysqlIdentifier f <> emit "=" <> fromMysqlExpression v

      let csfields = mysqlSepBy (emit ", ") $ fmap mysqlIdentifier fields

      let fast = do
            runNoReturn $ MysqlCommandSyntax $ fromMysqlInsert is

            -- Select inserted rows by Primary Keys
            -- Result can be totally wrong if some of (vals :: MysqlExpressionSyntax) can result in
            -- different values when evaluated by db.
            runReturningList $ MysqlCommandSyntax $
              emit "SELECT " <> csfields <> emit " FROM " <> fromMysqlTableName tn <> emit " WHERE "
                <> mysqlSepBy (emit " OR ") (mysqlSepBy (emit " AND ") . fmap equalTo . filter (flip elem pk . fst) . zip fields <$> vals)

      case aicol of
        Nothing -> fast -- no AI we can use PK to select inserted rows.
        Just ai ->
          if ai `notElem` pk
            then fast -- AI exists and not part of PK, so we don't care about it
            else do
              -- AI exists and is part of PK
              let tempTableName = emit "`_insert_returning_implementation`"

              runNoReturn $ MysqlCommandSyntax $
                emit "DROP TEMPORARY TABLE IF EXISTS " <> tempTableName

              runNoReturn $ MysqlCommandSyntax $
                emit "CREATE TEMPORARY TABLE " <> tempTableName <> emit " SELECT " <> csfields <> emit " FROM " <> fromMysqlTableName tn <> emit " LIMIT 0"

              forM_ vals $ \val -> do
                runNoReturn $ MysqlCommandSyntax
                  $ fromMysqlInsert
                  $ MysqlInsertSyntax tn fields (MysqlInsertValuesSyntax [val])

                -- hacky. But is there any other way to figure out if AI field is set to some value, or DEFAULT, for example?
                let compareMysqlExporessions a b =
                      toLazyByteString (unwrapInnerBuilder $ fromMysqlExpression a)
                        == toLazyByteString (unwrapInnerBuilder $ fromMysqlExpression b)

                let go (f, v) = (f, if f == ai && compareMysqlExporessions v defaultE then MysqlExpressionSyntax $ emit "LAST_INSERT_ID()" else v)

                -- Select inserted rows by Primary Keys
                -- Result can be totally wrong if some of (vals :: MysqlExpressionSyntax) can result in
                -- different values when evaluated by db.
                runNoReturn $ MysqlCommandSyntax $
                  emit "INSERT INTO " <> tempTableName <> emit " SELECT " <> csfields <> emit " FROM " <> fromMysqlTableName tn
                    <> emit " WHERE "
                    <> mysqlSepBy (emit " AND ") (fmap equalTo $ filter (flip elem pk . fst) $ zipWith (curry go) fields val)

              res <-
                runReturningList $ MysqlCommandSyntax $
                  emit "SELECT " <> csfields <> emit " FROM " <> tempTableName

              runNoReturn $ MysqlCommandSyntax $
                emit "DROP TEMPORARY TABLE " <> tempTableName

              pure res

instance Beam.MonadBeamInsertReturning MySQL MySQLM where
  runInsertReturningList = runInsertReturningList

runInsertRowReturning ::
  FromBackendRow MySQL (table Identity) =>
  SqlInsert MySQL table ->
  MySQLM (Maybe (table Identity))
runInsertRowReturning SqlInsertNoRows = pure Nothing
runInsertRowReturning (SqlInsert _ is@(MysqlInsertSyntax tn@(MysqlTableNameSyntax schema table) fields values)) =
  case values of
    MysqlInsertSelectSyntax _ -> fail "Not implemented runInsertReturningList part handling: INSERT INTO .. SELECT .."
    MysqlInsertValuesSyntax (_ : _ : _) -> fail "runInsertRowReturning can't be used to insert several rows"
    MysqlInsertValuesSyntax [] -> pure Nothing
    MysqlInsertValuesSyntax [vals] -> do
      let tableB = emit . TE.encodeUtf8Builder $ table
      let schemaB = emit $ TE.encodeUtf8Builder $ maybe "DATABASE()" (\s -> "'" <> s <> "'") schema
      (keycols :: [T.Text]) <-
        runReturningList $ MysqlCommandSyntax $
          emit "SELECT `column_name` FROM `information_schema`.`columns` WHERE "
            <> emit "`table_schema`="
            <> schemaB
            <> emit " AND `table_name`='"
            <> tableB
            <> emit "' AND `column_key` LIKE 'PRI'"
      let primaryKeyCols = keycols `intersect` fields
      when (null primaryKeyCols) $ fail "Table PK is not part of beam-table. Tables with no PK not allowed."
      (mautoIncrementCol :: Maybe T.Text) <-
        runReturningOne $ MysqlCommandSyntax $
          emit "SELECT `column_name` FROM `information_schema`.`columns` WHERE "
            <> emit "`table_schema`="
            <> schemaB
            <> emit " AND `table_name`='"
            <> tableB
            <> emit "' AND `extra` LIKE 'auto_increment'"
      let equalTo :: (T.Text, MysqlExpressionSyntax) -> MysqlSyntax
          equalTo (field, value) = mysqlIdentifier field <> emit "=" <> fromMysqlExpression value
      let fieldsExpr = mysqlSepBy (emit ", ") $ fmap mysqlIdentifier fields
      let selectByPrimaryKeyCols colValues =
            -- Select inserted rows by Primary Keys
            -- Result can be totally wrong if some of (vals :: MysqlExpressionSyntax) can result in
            -- different values when evaluated by db.
            runReturningOne $ MysqlCommandSyntax $
              emit "SELECT " <> fieldsExpr <> emit " FROM " <> fromMysqlTableName tn <> emit " WHERE "
                <> (mysqlSepBy (emit " AND ") . fmap equalTo . filter ((`elem` primaryKeyCols) . fst) $ colValues)
      let insertReturningWithoutAutoincrement = do
            runNoReturn $ MysqlCommandSyntax $ fromMysqlInsert is
            selectByPrimaryKeyCols $ zip fields vals
      case mautoIncrementCol of
        Nothing -> insertReturningWithoutAutoincrement -- no AI we can use PK to select inserted rows.
        Just aiCol ->
          if aiCol `notElem` primaryKeyCols
            then insertReturningWithoutAutoincrement -- AI exists and not part of PK, so we don't care about it
            else do
              -- AI exists and is part of PK
              runNoReturn $ MysqlCommandSyntax
                $ fromMysqlInsert
                $ MysqlInsertSyntax tn fields (MysqlInsertValuesSyntax [vals])
              -- hacky. But is there any other way to figure out if AI field is set to some value, or DEFAULT, for example?
              let compareMysqlExpressions a b =
                    toLazyByteString (unwrapInnerBuilder $ fromMysqlExpression a)
                      == toLazyByteString (unwrapInnerBuilder $ fromMysqlExpression b)
              let compareWithAutoincrement (field, value) =
                    ( field,
                      if field == aiCol && compareMysqlExpressions value defaultE
                        then MysqlExpressionSyntax $ emit "last_insert_id()"
                        else value
                    )
              selectByPrimaryKeyCols (zipWith (curry compareWithAutoincrement) fields vals)
