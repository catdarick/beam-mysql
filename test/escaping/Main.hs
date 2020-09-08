{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DerivingStrategies #-}

module Main (main) where

import Data.Text.Lazy (toStrict)
import Data.Text.Lazy.Encoding (decodeUtf8)
import Data.ByteString.Builder (toLazyByteString)
import GHC.Generics (Generic)
import Data.Functor.Identity (Identity)
import Database.Beam (Table (PrimaryKey, primaryKey), Columnar, Beamable,
                      TableEntity, Database, DatabaseSettings,
                      defaultDbSettings)
import Database.Beam.MySQL.Syntax (fromMysqlCommand, unwrapInnerBuilder,
                                   MysqlCommandSyntax,
                                   MysqlSyntax (MysqlSyntax))
import Database.Beam.Backend.SQL (insertCmd, selectCmd, updateCmd, deleteCmd)
import Database.Beam.Query (SqlInsert (SqlInsertNoRows, SqlInsert), 
                            SqlSelect (SqlSelect), QExpr, QBaseScope, 
                            SqlUpdate (SqlIdentityUpdate, SqlUpdate),
                            SqlDelete (SqlDelete),
                            QFieldAssignment, insert, insertValues,
                            filter_, select, all_, (==.), val_, updateTable,
                            toNewValue, delete)
import Data.Kind (Type)
import Data.Text (Text)
import Database.Beam.MySQL (MySQL)
import Data.Vector (Vector, fromList)
import Data.Foldable (traverse_)
import Test.Hspec (hspec, Spec, describe, it, shouldBe, beforeAll,
                   shouldReturn)
import Fmt ((+|), (|+))
import Database.MySQL.Temp (withTempDB, toConnectInfo)
import Control.Exception.Safe (bracket)
import Database.MySQL.Base (connect, close, Connection, escape, query)

main :: IO ()
main = withTempDB (\db -> do
  let info = toConnectInfo db
  bracket (connect info) 
           close 
           (\conn -> do
              -- query conn "create database test;"
              -- query conn "use test"
              hspec $ do
                bobbyTablesSpec conn))
              -- traverse_ (uncurry (escapingSpec conn')) escapeSequences))

-- Helpers

bobbyTablesSpec :: Connection -> Spec
bobbyTablesSpec conn = beforeAll (dumpInsertSQL conn bobbyTablesInsert) $ do
  describe "SQL injections" $ do
    it "should quote string literals to avoid SQL injection" $ \sql -> do
      sql `shouldBe` Just "foo"

bobbyTablesInsert :: SqlInsert MySQL TestT
bobbyTablesInsert = insert (_testTestTable testDB) . insertValues $ [
  TestT "Robert'); DROP TABLE Students;--"
  ]

dumpInsertSQL :: Connection -> SqlInsert MySQL TestT -> IO (Maybe Text)
dumpInsertSQL conn = \case
  SqlInsertNoRows -> pure Nothing
  SqlInsert _ ins -> Just <$> (render conn . insertCmd $ ins) 

render :: Connection -> MysqlCommandSyntax -> IO Text
render conn command = do
  let MysqlSyntax cmd = fromMysqlCommand command
  cmdBuilder <- cmd (\_ b _ -> pure b) (escape conn) mempty conn
  pure . toStrict . decodeUtf8 . toLazyByteString $ cmdBuilder

{-
bobbyTablesSpec :: Connection -> Spec
bobbyTablesSpec conn = describe "SQL injections" $ do
  it "should quote string literals to avoid SQL injection" $
    dumpInsertSQL conn bobbyTablesInsert `shouldBe` Just "foo"

render :: Connection -> MysqlCommandSyntax -> Text
render conn command = case fromMysqlCommand command of
  MysqlSyntax cmd -> let cmdBuilder = cmd (\_ b _ -> pure b) (escape conn) mempty conn in
                        _

render = toStrict . decodeUtf8 . toLazyByteString . unwrapInnerBuilder . fromMysqlCommand

dumpInsertSQL :: Connection -> SqlInsert MySQL TestT -> Maybe Text
dumpInsertSQL conn = \case
  SqlInsertNoRows -> Nothing
  SqlInsert _ ins -> Just . render conn . insertCmd $ ins
-}

{-
escapeSequences :: Vector (Text, Text)
escapeSequences = fromList [
    ("\'", "\'") -- A single quote (“'”) character
  , ("\"", "\"") -- A double quote (“"”) character
  , ("b", "\b")  -- A backspace character
  , ("n", "\n")  -- A newline (linefeed) character
  , ("r", "\r")  -- A carriage return character
  , ("t", "\t")  -- A tab character
  , ("\\", "\\") --	A backslash (“\”) character
  ]

escapingSpec :: Text -> Text -> Spec
escapingSpec repr escapeSeq = do
  describe ("'\\" +| repr |+ "' in generated SQL") $ do
    it "should be escaped in INSERT VALUES literals" $
      dumpInsertSQL (insertStmt escapeSeq) `shouldBe`
        Just ("INSERT INTO `test_table`(text) VALUES ('foo\\" +| repr |+ "')")
    it "should be escaped in SELECT WHERE literals" $
      dumpSelectSQL (selectWhereStmt escapeSeq) `shouldBe`
        ("SELECT `t0`.`text` AS `res0` FROM `test_table` AS `t0` WHERE (`t0`.`text`) = ('foo\\" +| repr |+ "')")
    it "should be escaped in UPDATE WHERE literals" $
      dumpUpdateSQL (updateWhereStmt escapeSeq) `shouldBe`
        Just ("UPDATE `test_table` SET `text`='bar\\" +| repr |+ "' WHERE (`text`) = ('foo\\" +| repr |+ "')")
    it "should be escaped in DELETE WHERE literals" $
      dumpDeleteSQL (deleteWhereStmt escapeSeq) `shouldBe`
        ("DELETE FROM `test_table` WHERE (`text`) = ('foo\\" +| repr |+ "')")

dumpSelectSQL :: SqlSelect MySQL (TestT Identity) -> Text
dumpSelectSQL (SqlSelect sel) = render . selectCmd $ sel

dumpUpdateSQL :: SqlUpdate MySQL TestT -> Maybe Text
dumpUpdateSQL = \case
  SqlIdentityUpdate -> Nothing
  SqlUpdate _ upd -> Just . render . updateCmd $ upd

dumpDeleteSQL :: SqlDelete MySQL TestT -> Text
dumpDeleteSQL (SqlDelete _ del) = render . deleteCmd $ del

insertStmt :: Text -> SqlInsert MySQL TestT
insertStmt escapeSeq =
  insert (_testTestTable testDB) . insertValues $ [
    TestT $ "foo" <> escapeSeq
    ]

selectWhereStmt :: Text -> SqlSelect MySQL (TestT Identity)
selectWhereStmt escapeSeq = select . filter_ go . all_ . _testTestTable $ testDB
  where
    go :: TestT (QExpr MySQL QBaseScope) -> QExpr MySQL QBaseScope Bool
    go row = _testText row ==. val_ ("foo" <> escapeSeq)

updateWhereStmt :: Text -> SqlUpdate MySQL TestT
updateWhereStmt escapeSeq = updateTable (_testTestTable testDB) upd wher
  where
    upd :: TestT (QFieldAssignment MySQL TestT)
    upd = TestT (toNewValue (val_ ("bar" +| escapeSeq |+ "")))
    wher :: forall s . TestT (QExpr MySQL s) -> QExpr MySQL s Bool
    wher row = _testText row ==. val_ ("foo" +| escapeSeq |+ "")

deleteWhereStmt :: Text -> SqlDelete MySQL TestT
deleteWhereStmt escapeSeq = delete (_testTestTable testDB) go
  where
    go :: (forall s' . TestT (QExpr MySQL s')) -> QExpr MySQL s Bool
    go row = _testText row ==. val_ ("foo" +| escapeSeq |+ "")

-}
newtype TestT (f :: Type -> Type) = TestT
  {
    _testText :: Columnar f Text
  }
  deriving stock (Generic)
  deriving anyclass (Beamable)

instance Table TestT where
  data PrimaryKey TestT (f :: Type -> Type) =
    TestTPK (Columnar f Text)
    deriving stock (Generic)
    deriving anyclass (Beamable)
  primaryKey = TestTPK . _testText

newtype TestDB (f :: Type -> Type) = TestDB
  {
    _testTestTable :: f (TableEntity TestT)
  }
  deriving stock (Generic)
  deriving anyclass (Database MySQL)

testDB :: DatabaseSettings MySQL TestDB
testDB = defaultDbSettings 
