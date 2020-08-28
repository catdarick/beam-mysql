{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE KindSignatures      #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.Beam.MySQL.FromField
(
  DecodeErrorTag (..),
  MySQLFieldDecode,
  FromField (..)
) where

import           Control.Exception.Safe (Exception, MonadThrow, throw)
import           Control.Monad.Reader (MonadReader (ask), ReaderT)
import           Data.Aeson (Value, eitherDecodeStrict)
import           Data.Bits (Bits (zeroBits))
import           Data.ByteString (ByteString)
import           Data.HashSet (HashSet)
import           Data.Int (Int16, Int32, Int64, Int8)
import           Data.IntCast (intCastMaybe)
import           Data.Kind (Type)
import           Data.Scientific (Scientific, toBoundedInteger)
import           Data.Text (Text, pack)
import           Data.Text.Encoding (encodeUtf8)
import           Data.Time (Day, LocalTime (LocalTime), NominalDiffTime,
                            daysAndTimeOfDayToTime, midnight)
import           Data.Word (Word16, Word32, Word64, Word8)
import           Database.Beam.Backend (SqlNull (SqlNull))
import           Database.MySQL.Protocol.ColumnDef (FieldType)
import           Database.MySQL.Protocol.MySQLValue (MySQLValue (..))

data DecodeErrorTag =
  UnexpectedNull |
  TypeMismatch |
  ValueWon'tFit |
  JSONParseFailed {-# UNPACK #-} !Text
  deriving stock (Eq, Show)

instance Exception DecodeErrorTag

-- TODO: Reduce unnecessary stuff from ReaderT. - Koz
newtype MySQLFieldDecode (m :: Type -> Type) (a :: Type) =
  MySQLFieldDecode (ReaderT (HashSet Text, FieldType, MySQLValue) m a)
  deriving newtype (Functor,
                    Applicative,
                    Monad,
                    MonadReader (HashSet Text, FieldType, MySQLValue),
                    MonadThrow)

class FromField (a :: Type) where
  fromField :: (MonadThrow m) => MySQLFieldDecode m a

instance FromField Bool where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLInt8 v'  -> pure (zeroBits /= v')
      MySQLInt8U v' -> pure (zeroBits /= v')
      MySQLBit v'   -> pure (zeroBits /= v')
      MySQLNull     -> throw UnexpectedNull
      _             -> throw TypeMismatch

instance FromField Int8 where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLInt8 v'   -> pure v'
      MySQLDecimal _ -> tryPack packDecimal
      MySQLNull      -> throw UnexpectedNull
      _              -> throw TypeMismatch

instance FromField Int16 where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLInt8 v'   -> pure . fromIntegral $ v'
      MySQLInt16 v'  -> pure v'
      MySQLDecimal _ -> tryPack packDecimal
      MySQLNull      -> throw UnexpectedNull
      _              -> throw TypeMismatch

instance FromField Int32 where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLInt8 v'   -> pure . fromIntegral $ v'
      MySQLInt16 v'  -> pure . fromIntegral $ v'
      MySQLInt32 v'  -> pure v'
      MySQLDecimal _ -> tryPack packDecimal
      MySQLNull      -> throw UnexpectedNull
      _              -> throw TypeMismatch

instance FromField Int64 where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLInt8 v'   -> pure . fromIntegral $ v'
      MySQLInt16 v'  -> pure . fromIntegral $ v'
      MySQLInt32 v'  -> pure . fromIntegral $ v'
      MySQLInt64 v'  -> pure v'
      MySQLDecimal _ -> tryPack packDecimal
      MySQLNull      -> throw UnexpectedNull
      _              -> throw TypeMismatch

instance FromField Int where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLInt8 v'   -> pure . fromIntegral $ v'
      MySQLInt16 v'  -> pure . fromIntegral $ v'
      MySQLInt32 _   -> tryPack packIntLike
      MySQLInt64 _   -> tryPack packIntLike
      MySQLDecimal _ -> tryPack packDecimal
      MySQLNull      -> throw UnexpectedNull
      _              -> throw TypeMismatch

instance FromField Word8 where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLInt8U v'  -> pure v'
      MySQLDecimal _ -> tryPack packDecimal
      MySQLNull      -> throw UnexpectedNull
      _              -> throw TypeMismatch

instance FromField Word16 where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLInt8U v'  -> pure . fromIntegral $ v'
      MySQLInt16U v' -> pure v'
      MySQLDecimal _ -> tryPack packDecimal
      MySQLNull      -> throw UnexpectedNull
      _              -> throw TypeMismatch

instance FromField Word32 where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLInt8U v'  -> pure . fromIntegral $ v'
      MySQLInt16U v' -> pure . fromIntegral $ v'
      MySQLInt32U v' -> pure v'
      MySQLDecimal _ -> tryPack packDecimal
      MySQLNull      -> throw UnexpectedNull
      _              -> throw TypeMismatch

instance FromField Word64 where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLInt8U v'  -> pure . fromIntegral $ v'
      MySQLInt16U v' -> pure . fromIntegral $ v'
      MySQLInt32U v' -> pure . fromIntegral $ v'
      MySQLInt64U v' -> pure v'
      MySQLDecimal _ -> tryPack packDecimal
      MySQLNull      -> throw UnexpectedNull
      _              -> throw TypeMismatch

instance FromField Word where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLInt8U v'  -> pure . fromIntegral $ v'
      MySQLInt16U v' -> pure . fromIntegral $ v'
      MySQLInt32U _  -> tryPack packUIntLike
      MySQLInt64U _  -> tryPack packUIntLike
      MySQLDecimal _ -> tryPack packDecimal
      MySQLNull      -> throw UnexpectedNull
      _              -> throw TypeMismatch

instance FromField Float where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLFloat v' -> pure v'
      MySQLNull     -> throw UnexpectedNull
      _             -> throw TypeMismatch

instance FromField Double where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLFloat v'  -> pure . realToFrac $ v'
      MySQLDouble v' -> pure v'
      MySQLNull      -> throw UnexpectedNull
      _              -> throw TypeMismatch

instance FromField Scientific where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLInt8 v'    -> pure . fromIntegral $ v'
      MySQLInt8U v'   -> pure . fromIntegral $ v'
      MySQLInt16 v'   -> pure . fromIntegral $ v'
      MySQLInt16U v'  -> pure . fromIntegral $ v'
      MySQLInt32 v'   -> pure . fromIntegral $ v'
      MySQLInt32U v'  -> pure . fromIntegral $ v'
      MySQLInt64 v'   -> pure . fromIntegral $ v'
      MySQLInt64U v'  -> pure . fromIntegral $ v'
      MySQLDecimal v' -> pure v'
      MySQLNull       -> throw UnexpectedNull
      _               -> throw TypeMismatch

instance FromField Rational where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLInt8 v'   -> pure . fromIntegral $ v'
      MySQLInt8U v'  -> pure . fromIntegral $ v'
      MySQLInt16 v'  -> pure . fromIntegral $ v'
      MySQLInt16U v' -> pure . fromIntegral $ v'
      MySQLInt32 v'  -> pure . fromIntegral $ v'
      MySQLInt32U v' -> pure . fromIntegral $ v'
      MySQLInt64 v'  -> pure . fromIntegral $ v'
      MySQLInt64U v' -> pure . fromIntegral $ v'
      MySQLNull      -> throw UnexpectedNull
      _              -> throw TypeMismatch

instance FromField SqlNull where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLNull -> pure SqlNull
      _         -> throw TypeMismatch

instance FromField Text where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLText v' -> pure v'
      MySQLNull    -> throw UnexpectedNull
      _            -> throw TypeMismatch

instance FromField ByteString where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLText v'  -> pure . encodeUtf8 $ v'
      MySQLBytes v' -> pure v'
      MySQLNull     -> throw UnexpectedNull
      _             -> throw TypeMismatch

instance FromField LocalTime where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLDateTime v'  -> pure v'
      MySQLTimeStamp v' -> pure v'
      MySQLDate v'      -> pure . LocalTime v' $ midnight
      MySQLNull         -> throw UnexpectedNull
      _                 -> throw TypeMismatch

instance FromField Day where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLDate v' -> pure v'
      MySQLNull    -> throw UnexpectedNull
      _            -> throw TypeMismatch

instance FromField NominalDiffTime where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLTime s v' -> do
        let isPositive = s == zeroBits
        let ndt = daysAndTimeOfDayToTime 0 v'
        pure $ if isPositive then ndt else negate ndt
      MySQLNull      -> throw UnexpectedNull
      _              -> throw TypeMismatch

instance FromField Value where
  {-# INLINABLE fromField #-}
  fromField = do
    (_, _, v) <- ask
    case v of
      MySQLText _  -> tryParseJSON extractJSON
      MySQLBytes _ -> tryParseJSON extractJSON
      MySQLNull    -> throw UnexpectedNull
      _            -> throw TypeMismatch

-- Helpers

-- Packers

packDecimal :: (Integral a, Bounded a) => MySQLValue -> Maybe a
packDecimal = \case
  MySQLDecimal v -> toBoundedInteger v
  _ -> Nothing

packIntLike :: (Integral a, Bits a) => MySQLValue -> Maybe a
packIntLike = \case
  MySQLInt32 v -> intCastMaybe v
  MySQLInt64 v -> intCastMaybe v
  _ -> Nothing

packUIntLike :: (Integral a, Bits a) => MySQLValue -> Maybe a
packUIntLike = \case
  MySQLInt32U v -> intCastMaybe v
  MySQLInt64U v -> intCastMaybe v
  _ -> Nothing

-- Generalized packing helper
tryPack :: (MonadThrow m) => (MySQLValue -> Maybe a) -> MySQLFieldDecode m a
tryPack packer = do
  (_, _, v) <- ask
  case packer v of
    Nothing -> throw ValueWon'tFit
    Just v' -> pure v'

extractJSON :: MySQLValue -> Maybe ByteString
extractJSON = \case
  MySQLText v' -> Just . encodeUtf8 $ v'
  MySQLBytes v' -> Just v'
  _ -> Nothing

tryParseJSON :: (MonadThrow m) =>
  (MySQLValue -> Maybe ByteString) -> MySQLFieldDecode m Value
tryParseJSON extractor = do
  (_, _, v) <- ask
  case eitherDecodeStrict <$> extractor v of
    Nothing         -> throw TypeMismatch
    Just (Left err) -> throw . JSONParseFailed . pack $ err
    Just (Right v') -> pure v'
