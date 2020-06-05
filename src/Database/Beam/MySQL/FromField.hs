{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.Beam.MySQL.FromField
  ( FromField (..),
    runFieldParser,
  )
where

import Control.Applicative ((<|>))
import Control.Monad.Except
  ( ExceptT,
    MonadError,
    runExceptT,
    throwError,
  )
import Control.Monad.IO.Class (MonadIO)
import Data.Aeson (Value, eitherDecodeStrict)
import Data.Attoparsec.ByteString.Char8
  ( Parser,
    char,
    decimal,
    digit,
    double,
    parseOnly,
    rational,
    signed,
  )
import Data.Bifunctor (first)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as SB
import qualified Data.ByteString.Lazy.Char8 as LB
import Data.Char (digitToInt)
import Data.Fixed (showFixed)
import Data.Int (Int16, Int32, Int64, Int8)
import Data.Ratio (Ratio)
import Data.Scientific (Scientific)
import qualified Data.Text as TS
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Lazy as TL
import Data.Time.Calendar (Day, fromGregorianValid)
import Data.Time.Clock (NominalDiffTime)
import Data.Time.LocalTime
  ( LocalTime (LocalTime),
    TimeOfDay,
    makeTimeOfDayValid,
  )
import Data.Word (Word16, Word32, Word64, Word8)
import Database.Beam.Backend.SQL (SqlNull (SqlNull))
import Database.Beam.Backend.SQL.Row
  ( ColumnParseError
      ( ColumnTypeMismatch,
        ColumnUnexpectedNull
      ),
  )
import Database.MySQL.Base.Types
  ( Field,
    Type
      ( Bit,
        Blob,
        Date,
        DateTime,
        Decimal,
        Double,
        Enum,
        Float,
        Int24,
        Long,
        LongBlob,
        LongLong,
        MediumBlob,
        NewDecimal,
        Short,
        String,
        Time,
        Timestamp,
        Tiny,
        TinyBlob,
        VarChar,
        VarString
      ),
    fieldType,
  )
import Text.Printf (printf)
import Type.Reflection (TypeRep, Typeable, typeRep)

newtype FieldParser a = FP (ExceptT ColumnParseError IO a)
  deriving newtype
    ( Functor,
      Applicative,
      Monad,
      MonadIO,
      MonadError ColumnParseError
    )

runFieldParser :: FieldParser a -> IO (Either ColumnParseError a)
runFieldParser (FP comp) = runExceptT comp

class FromField a where
  fromField :: Field -> Maybe SB.ByteString -> FieldParser a

instance FromField Bool where
  fromField f = \case
    Nothing -> unexpectedNull f ""
    md@(Just d) -> (0 /=) <$> case fieldType f of
      Tiny -> fromField f md
      Bit -> case BS.uncons d of
        Nothing -> conversionFailed f (show d)
        Just (h, _) -> pure h
      _ -> incompatibleTypes f ""

instance FromField Word where
  fromField = atto check64 decimal

instance FromField Word8 where
  fromField = atto check8 decimal

instance FromField Word16 where
  fromField = atto check16 decimal

instance FromField Word32 where
  fromField = atto check32 decimal

instance FromField Word64 where
  fromField = atto check64 decimal

instance FromField Int where
  fromField = atto check64 (signed decimal)

instance FromField Int8 where
  fromField = atto check8 (signed decimal)

instance FromField Int16 where
  fromField = atto check16 (signed decimal)

instance FromField Int32 where
  fromField = atto check32 (signed decimal)

instance FromField Int64 where
  fromField = atto check64 (signed decimal)

instance FromField Float where
  fromField = atto check (realToFrac <$> double)
    where
      check = \case
        Int24 -> True
        Float -> True
        Decimal -> True
        NewDecimal -> True
        Double -> True
        ty -> check16 ty

instance FromField Double where
  fromField = atto check double
    where
      check = \case
        Float -> True
        Double -> True
        Decimal -> True
        NewDecimal -> True
        ty -> check32 ty

instance FromField Scientific where
  fromField = atto checkScientific rational

instance FromField (Ratio Integer) where
  fromField = atto checkScientific rational

instance (FromField a) => FromField (Maybe a) where
  fromField field = \case
    Nothing -> pure Nothing
    md -> Just <$> fromField field md

instance FromField SqlNull where
  fromField f = \case
    Nothing -> pure SqlNull
    _ ->
      throwError
        ( ColumnTypeMismatch
            "SqlNull"
            (show . fieldType $ f)
            "Non-null value found"
        )

instance FromField SB.ByteString where
  fromField = doConvert checkBytes pure

instance FromField LB.ByteString where
  fromField f d = LB.fromChunks . pure <$> fromField f d

instance FromField TS.Text where
  fromField = doConvert checkText (first show . TE.decodeUtf8')

instance FromField TL.Text where
  fromField f d = TL.fromChunks . pure <$> fromField f d

instance FromField LocalTime where
  fromField = atto checkDate localTime
    where
      checkDate = \case
        DateTime -> True
        Timestamp -> True
        Date -> True
        _ -> False
      localTime = do
        (day, time) <- dayAndTime
        pure (LocalTime day time)

instance FromField Day where
  fromField = atto checkDay dayP
    where
      checkDay = \case
        Date -> True
        _ -> False

instance FromField TimeOfDay where
  fromField = atto checkTime timeP

instance FromField NominalDiffTime where
  fromField = atto checkTime durationP

instance FromField Value where
  fromField f = \case
    Nothing -> conversionFailed f "Failed to extract JSON bytes."
    Just bs -> case eitherDecodeStrict bs of
      Left err -> conversionFailed f err
      Right x -> pure x

-- Helpers

-- Pre-checkers for specific types we can fit into various Haskell locations
check8 :: Type -> Bool
check8 = \case
  Tiny -> True
  NewDecimal -> True
  _ -> False

check16 :: Type -> Bool
check16 = \case
  Short -> True
  ty -> check8 ty

check32 :: Type -> Bool
check32 = \case
  Int24 -> True
  Long -> True
  ty -> check16 ty

check64 :: Type -> Bool
check64 = \case
  LongLong -> True
  ty -> check32 ty

checkScientific :: Type -> Bool
checkScientific = \case
  Float -> True
  Double -> True
  Decimal -> True
  NewDecimal -> True
  ty -> check64 ty

checkText :: Type -> Bool
checkText = \case
  VarChar -> True
  VarString -> True
  String -> True
  Enum -> True
  Blob -> True
  _ -> False

checkBytes :: Type -> Bool
checkBytes = \case
  TinyBlob -> True
  MediumBlob -> True
  LongBlob -> True
  Blob -> True
  ty -> checkText ty

checkTime :: Type -> Bool
checkTime = \case
  Time -> True
  _ -> False

-- Pre-checker helper
atto ::
  Typeable a =>
  (Type -> Bool) ->
  Parser a ->
  Field ->
  Maybe SB.ByteString ->
  FieldParser a
atto checkType parser = doConvert checkType (parseOnly parser)

-- Conversion driver
doConvert ::
  Typeable a =>
  (Type -> Bool) ->
  (SB.ByteString -> Either String a) ->
  Field ->
  Maybe SB.ByteString ->
  FieldParser a
doConvert checkType parser field = \case
  Nothing -> unexpectedNull field ""
  Just d ->
    if checkType . fieldType $ field
      then case parser d of
        Left err -> conversionFailed field err
        Right r -> pure r
      else incompatibleTypes field ""

-- Parser helpers

-- Parse hours, minutes, seconds and microseconds
parseHMSU ::
  forall a b c d.
  (Num a, Num b, Num c, Num d) =>
  Parser (a, b, c, d)
parseHMSU = do
  hour <- lengthedDecimal 2
  _ <- char ':'
  minute <- lengthedDecimal 2
  _ <- char ':'
  seconds <- lengthedDecimal 2
  microseconds <- (char '.' *> maxLengthedDecimal 6) <|> pure 0
  pure (hour, minute, seconds, microseconds)

dayAndTime :: Parser (Day, TimeOfDay)
dayAndTime = do
  day <- dayP
  _ <- char ' '
  time <- timeP
  pure (day, time)

timeP :: Parser TimeOfDay
timeP = do
  (hour, minute, seconds, microseconds) <- parseHMSU
  let pico = seconds + microseconds * 1e-6
  case makeTimeOfDayValid hour minute pico of
    Nothing ->
      fail
        ( printf
            "Invalid time part: %02d:%02d:%s"
            hour
            minute
            (showFixed False pico)
        )
    Just tod -> pure tod

-- TODO: check how are hours parsed (was <- lenghedDecimal 3 in the original repo)
durationP :: Parser NominalDiffTime
durationP = do
  negative <- (True <$ char '-') <|> pure False
  (hour, minute, seconds, microseconds) <- parseHMSU
  let v =
        hour * 3600 + minute * 60 + seconds
          + microseconds
          * 1e-6
  pure (if negative then negate v else v)

dayP :: Parser Day
dayP = do
  year <- lengthedDecimal 4
  _ <- char '-'
  month <- lengthedDecimal 2
  _ <- char '-'
  day <- lengthedDecimal 2
  case fromGregorianValid year month day of
    Nothing -> fail (printf "Invalid date part: %04d-%02d-%02d" year month day)
    Just day' -> pure day'

-- Parse until you hit a certain maximum number of decimals
maxLengthedDecimal :: Num a => Int -> Parser a
maxLengthedDecimal = go1 0
  where
    go1 a n = do
      d <- digitToInt <$> digit
      go' (a * 10 + fromIntegral d) (n - 1)
    go' !a 0 = pure a
    go' !a n = go1 a n <|> pure (a * 10 ^ n)

-- Parse from a fixed number of decimals
lengthedDecimal :: Num a => Int -> Parser a
lengthedDecimal = lengthedDecimal' 0
  where
    lengthedDecimal' !a 0 = pure a
    lengthedDecimal' !a n = do
      d <- digitToInt <$> digit
      lengthedDecimal' (a * 10 + fromIntegral d) (n - 1)

-- Parse error helpers

incompatibleTypes ::
  forall a.
  (Typeable a) =>
  Field ->
  String ->
  FieldParser a
incompatibleTypes f msg =
  throwError
    ( ColumnTypeMismatch
        (show (typeRep :: TypeRep a))
        (show . fieldType $ f)
        msg
    )

unexpectedNull :: forall a. Field -> String -> FieldParser a
unexpectedNull _ _ = throwError ColumnUnexpectedNull

conversionFailed :: forall a. (Typeable a) => Field -> String -> FieldParser a
conversionFailed f msg =
  throwError
    ( ColumnTypeMismatch
        (show (typeRep :: TypeRep a))
        (show . fieldType $ f)
        msg
    )
