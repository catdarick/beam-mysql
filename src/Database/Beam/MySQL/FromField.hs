{-# LANGUAGE MultiWayIf #-}

module Database.Beam.MySQL.FromField (FromField (..)) where

import           Data.Aeson (Value, eitherDecodeStrict')
import           Data.Bits (finiteBitSize, zeroBits)
import           Data.ByteString (ByteString)
import           Data.Int (Int16, Int32, Int64, Int8)
import           Data.Scientific (Scientific, toBoundedInteger)
import           Data.Text (Text)
import           Data.Text.Encoding (encodeUtf8)
import           Data.Time.Calendar (Day)
import           Data.Time.Clock (NominalDiffTime)
import           Data.Time.LocalTime (LocalTime (..), TimeOfDay,
                                      daysAndTimeOfDayToTime, midnight)
import           Data.Word (Word16, Word32, Word64, Word8)
import           Database.Beam.Backend.SQL (SqlNull (..))
import           Database.Beam.Backend.SQL.Row (ColumnParseError (..))
import           Database.MySQL.Protocol.ColumnDef (FieldType, mySQLTypeBit,
                                                    mySQLTypeBlob,
                                                    mySQLTypeDate,
                                                    mySQLTypeDateTime,
                                                    mySQLTypeDateTime2,
                                                    mySQLTypeDecimal,
                                                    mySQLTypeDouble,
                                                    mySQLTypeEnum,
                                                    mySQLTypeFloat,
                                                    mySQLTypeInt24,
                                                    mySQLTypeLong,
                                                    mySQLTypeLongBlob,
                                                    mySQLTypeLongLong,
                                                    mySQLTypeMediumBlob,
                                                    mySQLTypeNewDate,
                                                    mySQLTypeNewDecimal,
                                                    mySQLTypeNull, mySQLTypeSet,
                                                    mySQLTypeShort,
                                                    mySQLTypeString,
                                                    mySQLTypeTime,
                                                    mySQLTypeTime2,
                                                    mySQLTypeTimestamp,
                                                    mySQLTypeTimestamp2,
                                                    mySQLTypeTiny,
                                                    mySQLTypeTinyBlob,
                                                    mySQLTypeVarChar,
                                                    mySQLTypeVarString,
                                                    mySQLTypeYear)
import           Database.MySQL.Protocol.MySQLValue (MySQLValue (..))

class FromField a where
  fromField :: FieldType -> MySQLValue -> Either ColumnParseError a

instance FromField Bool where
  fromField t = \case
    MySQLInt8 v  -> pure (zeroBits /= v)
    MySQLInt8U v -> pure (zeroBits /= v)
    MySQLBit v   -> pure (zeroBits /= v)
    _             -> throwTypeMismatch "Bool" t

instance FromField Int8 where
  fromField t = \case
    MySQLInt8 v    -> pure v
    MySQLDecimal v -> tryPackDecimal "Int8" v
    _               -> throwTypeMismatch "Int8" t

instance FromField Int16 where
  fromField t = \case
    MySQLInt8 v    -> pure . fromIntegral $ v
    MySQLInt16 v   -> pure v
    MySQLDecimal v -> tryPackDecimal "Int16" v
    _               -> throwTypeMismatch "Int16" t

instance FromField Int32 where
  fromField t = \case
    MySQLInt8 v    -> pure . fromIntegral $ v
    MySQLInt16 v   -> pure . fromIntegral $ v
    MySQLInt32 v   -> pure v
    MySQLDecimal v -> tryPackDecimal "Int32" v
    _               -> throwTypeMismatch "Int32" t

instance FromField Int64 where
  fromField t = \case
    MySQLInt8 v    -> pure . fromIntegral $ v
    MySQLInt16 v   -> pure . fromIntegral $ v
    MySQLInt32 v   -> pure . fromIntegral $ v
    MySQLInt64 v   -> pure v
    MySQLDecimal v -> tryPackDecimal "Int64" v
    _               -> throwTypeMismatch "Int64" t

instance FromField Int where
  fromField t = \case
    MySQLInt8 v -> pure . fromIntegral $ v
    MySQLInt16 v -> pure . fromIntegral $ v
    MySQLInt32 v -> pure . fromIntegral $ v
    MySQLInt64 v ->
      if finiteBitSize (zeroBits :: Int) == 64
        then pure . fromIntegral $ v
        else throwTypeMismatch "Int" t
    MySQLDecimal v -> tryPackDecimal "Int" v
    _ -> throwTypeMismatch "Int" t

instance FromField Word8 where
  fromField t = \case
    MySQLInt8U v   -> pure v
    MySQLDecimal v -> tryPackDecimal "Word8" v
    _               -> throwTypeMismatch "Word8" t

instance FromField Word16 where
  fromField t = \case
    MySQLInt8U v   -> pure . fromIntegral $ v
    MySQLInt16U v  -> pure v
    MySQLDecimal v -> tryPackDecimal "Word16" v
    _               -> throwTypeMismatch "Word16" t

instance FromField Word32 where
  fromField t = \case
    MySQLInt8U v   -> pure . fromIntegral $ v
    MySQLInt16U v  -> pure . fromIntegral $ v
    MySQLInt32U v  -> pure v
    MySQLDecimal v -> tryPackDecimal "Word32" v
    _               -> throwTypeMismatch "Word32" t

instance FromField Word64 where
  fromField t = \case
    MySQLInt8U v  -> pure . fromIntegral $ v
    MySQLInt16U v  -> pure . fromIntegral $ v
    MySQLInt32U v  -> pure . fromIntegral $ v
    MySQLInt64U v  -> pure v
    MySQLDecimal v -> tryPackDecimal "Word64" v
    _               -> throwTypeMismatch "Word64" t

instance FromField Word where
  fromField t = \case
    MySQLInt8U v -> pure . fromIntegral $ v
    MySQLInt16U v -> pure . fromIntegral $ v
    MySQLInt32U v -> pure . fromIntegral $ v
    MySQLInt64U v ->
      if finiteBitSize (zeroBits :: Word) == 64
      then pure . fromIntegral $ v
      else throwTypeMismatch "Word" t
    _ -> throwTypeMismatch "Word" t

instance FromField Float where
  fromField t = \case
    MySQLFloat v -> pure v
    _             -> throwTypeMismatch "Float" t

instance FromField Double where
  fromField t = \case
    MySQLFloat v  -> pure . realToFrac $ v
    MySQLDouble v -> pure v
    _              -> throwTypeMismatch "Double" t

instance FromField Scientific where
  fromField t = \case
    MySQLDecimal v -> pure v
    MySQLInt8 v    -> pure . fromIntegral $ v
    MySQLInt8U v   -> pure . fromIntegral $ v
    MySQLInt16 v   -> pure . fromIntegral $ v
    MySQLInt16U v  -> pure . fromIntegral $ v
    MySQLInt32 v   -> pure . fromIntegral $ v
    MySQLInt32U v  -> pure . fromIntegral $ v
    MySQLInt64 v   -> pure . fromIntegral $ v
    MySQLInt64U v  -> pure . fromIntegral $ v
    _               -> throwTypeMismatch "Scientific" t

instance FromField Rational where
  fromField t = \case
    MySQLInt8 v   -> pure . fromIntegral $ v
    MySQLInt8U v  -> pure . fromIntegral $ v
    MySQLInt16 v  -> pure . fromIntegral $ v
    MySQLInt16U v -> pure . fromIntegral $ v
    MySQLInt32 v  -> pure . fromIntegral $ v
    MySQLInt32U v -> pure . fromIntegral $ v
    MySQLInt64 v  -> pure . fromIntegral $ v
    MySQLInt64U v -> pure . fromIntegral $ v
    _              -> throwTypeMismatch "Rational" t

instance (FromField a) => FromField (Maybe a) where
  fromField t v = case v of
    MySQLNull -> pure Nothing
    _         -> Just <$> fromField t v

instance FromField SqlNull where
  fromField v = \case
    MySQLNull -> pure SqlNull
    _         -> Left . ColumnErrorInternal $ "Unexpected non-NULL, but got " <> show v

instance FromField ByteString where
  fromField t = \case
    MySQLText v  -> pure . encodeUtf8 $ v
    MySQLBytes v -> pure v
    _             -> throwTypeMismatch "ByteString" t

instance FromField Text where
  fromField t = \case
    MySQLText v -> pure v
    _            -> throwTypeMismatch "Text" t

instance FromField LocalTime where
  fromField t = \case
    MySQLDateTime v  -> pure v
    MySQLTimeStamp v -> pure v
    MySQLDate v      -> pure (LocalTime v midnight)
    _                 -> throwTypeMismatch "LocalTime" t

instance FromField Day where
  fromField t = \case
    MySQLDate v -> pure v
    _            -> throwTypeMismatch "Day" t

instance FromField TimeOfDay where
  fromField t = \case
    MySQLTime s v ->
      if s == zeroBits
        then pure v
        else throwTypeMismatch "TimeOfDay" t
    _ -> throwTypeMismatch "TimeOfDay" t

instance FromField NominalDiffTime where
  fromField t = \case
    MySQLTime s v -> do
      let isPositive = s == zeroBits
      let ndt = daysAndTimeOfDayToTime 0 v
      if isPositive
        then pure ndt
        else pure . negate $ ndt
    _ -> throwTypeMismatch "NominalDiffTime" t

instance FromField Value where
  fromField t = \case
    MySQLText v  -> case eitherDecodeStrict' . encodeUtf8 $ v of
      Left err  -> Left . ColumnErrorInternal $ "JSON parsing failed: " <> err
      Right val -> pure val
    MySQLBytes v -> case eitherDecodeStrict' v of
      Left err  -> Left . ColumnErrorInternal $ "JSON parsing failed: " <> err
      Right val -> pure val
    _             -> throwTypeMismatch "Aeson.Value" t

-- Helpers

tryPackDecimal :: (Integral a, Bounded a) =>
  String -> Scientific -> Either ColumnParseError a
tryPackDecimal typeName = maybe throwDecimalWon'tFit pure . toBoundedInteger
  where
    throwDecimalWon'tFit =
      Left . ColumnErrorInternal $ typeName <> " cannot store this DECIMAL"

throwTypeMismatch :: String -> FieldType -> Either ColumnParseError a
throwTypeMismatch typeName ft =
  Left . ColumnTypeMismatch typeName (fieldTypeToString ft) $ ""

-- Stringification of type names
fieldTypeToString :: FieldType -> String
fieldTypeToString ft = "MySQL " <>
  if | ft == mySQLTypeDecimal -> "Decimal"
     | ft == mySQLTypeTiny -> "Tiny"
     | ft == mySQLTypeShort -> "Short"
     | ft == mySQLTypeLong -> "Long"
     | ft == mySQLTypeFloat -> "Float"
     | ft == mySQLTypeDouble -> "Double"
     | ft == mySQLTypeNull -> "Null"
     | ft == mySQLTypeTimestamp -> "Timestamp"
     | ft == mySQLTypeLongLong -> "LongLong"
     | ft == mySQLTypeInt24 -> "Int24"
     | ft == mySQLTypeDate -> "Date"
     | ft == mySQLTypeTime -> "Time"
     | ft == mySQLTypeDateTime -> "DateTime"
     | ft == mySQLTypeYear -> "Year"
     | ft == mySQLTypeNewDate -> "NewDate"
     | ft == mySQLTypeVarChar -> "VarChar"
     | ft == mySQLTypeBit -> "Bit"
     | ft == mySQLTypeTimestamp2 -> "Timestamp2"
     | ft == mySQLTypeDateTime2 -> "DateTime2"
     | ft == mySQLTypeTime2 -> "Time2"
     | ft == mySQLTypeNewDecimal -> "NewDecimal"
     | ft == mySQLTypeEnum -> "Enum"
     | ft == mySQLTypeSet -> "Set"
     | ft == mySQLTypeTinyBlob -> "TinyBlob"
     | ft == mySQLTypeMediumBlob -> "MediumBlob"
     | ft == mySQLTypeLongBlob -> "LongBlob"
     | ft == mySQLTypeBlob -> "Blob"
     | ft == mySQLTypeVarString -> "VarString"
     | ft == mySQLTypeString -> "String"
     | otherwise -> "Geometry" -- brittle, to fix
