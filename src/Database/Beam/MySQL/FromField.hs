{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DerivingVia         #-}
{-# LANGUAGE KindSignatures      #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.Beam.MySQL.FromField
(
  DecodeErrorTag (..),
  MySQLFieldDecode,
  FromField (..)
) where

import           Control.Monad.Except (Except, MonadError (throwError))
import           Control.Monad.Reader (ReaderT (ReaderT))
import           Data.Aeson (Value, eitherDecodeStrict)
import           Data.Bits (Bits (zeroBits))
import           Data.ByteString (ByteString)
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
import           Database.MySQL.Protocol.MySQLValue (MySQLValue (..))

data DecodeErrorTag =
  UnexpectedNull |
  TypeMismatch |
  ValueWon'tFit |
  JSONParseFailed {-# UNPACK #-} !Text
  deriving stock (Eq, Show)

newtype MySQLFieldDecode (a :: Type) =
  MySQLFieldDecode (MySQLValue -> Except DecodeErrorTag a)
  deriving (Functor, Applicative, Monad)
    via (ReaderT MySQLValue (Except DecodeErrorTag))

class FromField (a :: Type) where
  fromField :: MySQLFieldDecode a

instance FromField Bool where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
      MySQLInt8 v  -> pure (zeroBits /= v)
      MySQLInt8U v -> pure (zeroBits /= v)
      MySQLBit v   -> pure (zeroBits /= v)
      MySQLNull    -> throwError UnexpectedNull
      _            -> throwError TypeMismatch

instance FromField Int8 where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLInt8 v    -> pure v
    MySQLDecimal v -> tryPack toBoundedInteger v
    MySQLNull      -> throwError UnexpectedNull
    _              -> throwError TypeMismatch

instance FromField Int16 where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLInt8 v    -> pure . fromIntegral $ v
    MySQLInt16 v   -> pure v
    MySQLDecimal v -> tryPack toBoundedInteger v
    MySQLNull      -> throwError UnexpectedNull
    _              -> throwError TypeMismatch

instance FromField Int32 where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLInt8 v    -> pure . fromIntegral $ v
    MySQLInt16 v   -> pure . fromIntegral $ v
    MySQLInt32 v   -> pure v
    MySQLDecimal v -> tryPack toBoundedInteger v
    MySQLNull      -> throwError UnexpectedNull
    _              -> throwError TypeMismatch

instance FromField Int64 where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLInt8 v    -> pure . fromIntegral $ v
    MySQLInt16 v   -> pure . fromIntegral $ v
    MySQLInt32 v   -> pure . fromIntegral $ v
    MySQLInt64 v   -> pure v
    MySQLDecimal v -> tryPack toBoundedInteger v
    MySQLNull      -> throwError UnexpectedNull
    _              -> throwError TypeMismatch

instance FromField Int where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLInt8 v    -> pure . fromIntegral $ v
    MySQLInt16 v   -> pure . fromIntegral $ v
    MySQLInt32 v   -> tryPack intCastMaybe v
    MySQLInt64 v   -> tryPack intCastMaybe v
    MySQLDecimal v -> tryPack toBoundedInteger v
    MySQLNull      -> throwError UnexpectedNull
    _              -> throwError TypeMismatch

instance FromField Word8 where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLInt8U v   -> pure v
    MySQLDecimal v -> tryPack toBoundedInteger v
    MySQLNull      -> throwError UnexpectedNull
    _              -> throwError TypeMismatch

instance FromField Word16 where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLInt8U v   -> pure . fromIntegral $ v
    MySQLInt16U v  -> pure v
    MySQLDecimal v -> tryPack toBoundedInteger v
    MySQLNull      -> throwError UnexpectedNull
    _              -> throwError TypeMismatch

instance FromField Word32 where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLInt8U v   -> pure . fromIntegral $ v
    MySQLInt16U v  -> pure . fromIntegral $ v
    MySQLInt32U v  -> pure v
    MySQLDecimal v -> tryPack toBoundedInteger v
    MySQLNull      -> throwError UnexpectedNull
    _              -> throwError TypeMismatch

instance FromField Word64 where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLInt8U v   -> pure . fromIntegral $ v
    MySQLInt16U v  -> pure . fromIntegral $ v
    MySQLInt32U v  -> pure . fromIntegral $ v
    MySQLInt64U v  -> pure v
    MySQLDecimal v -> tryPack toBoundedInteger v
    MySQLNull      -> throwError UnexpectedNull
    _              -> throwError TypeMismatch

instance FromField Word where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLInt8U v   -> pure . fromIntegral $ v
    MySQLInt16U v  -> pure . fromIntegral $ v
    MySQLInt32U v  -> tryPack intCastMaybe v
    MySQLInt64U v  -> tryPack intCastMaybe v
    MySQLDecimal v -> tryPack toBoundedInteger v
    MySQLNull      -> throwError UnexpectedNull
    _              -> throwError TypeMismatch

instance FromField Float where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLFloat v  -> pure v
    MySQLNull     -> throwError UnexpectedNull
    _             -> throwError TypeMismatch

instance FromField Double where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLFloat v   -> pure . realToFrac $ v
    MySQLDouble v  -> pure v
    MySQLNull      -> throwError UnexpectedNull
    _              -> throwError TypeMismatch

instance FromField Scientific where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLInt8 v     -> pure . fromIntegral $ v
    MySQLInt8U v    -> pure . fromIntegral $ v
    MySQLInt16 v    -> pure . fromIntegral $ v
    MySQLInt16U v   -> pure . fromIntegral $ v
    MySQLInt32 v    -> pure . fromIntegral $ v
    MySQLInt32U v   -> pure . fromIntegral $ v
    MySQLInt64 v    -> pure . fromIntegral $ v
    MySQLInt64U v   -> pure . fromIntegral $ v
    MySQLDecimal v  -> pure v
    MySQLNull       -> throwError UnexpectedNull
    _               -> throwError TypeMismatch

instance FromField Rational where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLInt8 v    -> pure . fromIntegral $ v
    MySQLInt8U v   -> pure . fromIntegral $ v
    MySQLInt16 v   -> pure . fromIntegral $ v
    MySQLInt16U v  -> pure . fromIntegral $ v
    MySQLInt32 v   -> pure . fromIntegral $ v
    MySQLInt32U v  -> pure . fromIntegral $ v
    MySQLInt64 v   -> pure . fromIntegral $ v
    MySQLInt64U v  -> pure . fromIntegral $ v
    MySQLNull      -> throwError UnexpectedNull
    _              -> throwError TypeMismatch

instance FromField SqlNull where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLNull -> pure SqlNull
    _         -> throwError TypeMismatch

instance FromField Text where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLText v  -> pure v
    MySQLNull    -> throwError UnexpectedNull
    _            -> throwError TypeMismatch

instance FromField ByteString where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLText v   -> pure . encodeUtf8 $ v
    MySQLBytes v  -> pure v
    MySQLNull     -> throwError UnexpectedNull
    _             -> throwError TypeMismatch

instance FromField LocalTime where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLDateTime v   -> pure v
    MySQLTimeStamp v  -> pure v
    MySQLDate v       -> pure . LocalTime v $ midnight
    MySQLNull         -> throwError UnexpectedNull
    _                 -> throwError TypeMismatch

instance FromField Day where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLDate v  -> pure v
    MySQLNull    -> throwError UnexpectedNull
    _            -> throwError TypeMismatch

instance FromField NominalDiffTime where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLTime s v  -> do
      let isPositive = s == zeroBits
      let ndt = daysAndTimeOfDayToTime 0 v
      pure $ if isPositive then ndt else negate ndt
    MySQLNull      -> throwError UnexpectedNull
    _              -> throwError TypeMismatch

instance FromField Value where
  {-# INLINABLE fromField #-}
  fromField = MySQLFieldDecode $ \case
    MySQLText v  -> tryParseJSON encodeUtf8 v
    MySQLBytes v -> tryParseJSON id v
    MySQLNull    -> throwError UnexpectedNull
    _            -> throwError TypeMismatch

-- Helpers

tryPack :: (MonadError DecodeErrorTag m) => (a -> Maybe b) -> a -> m b
tryPack packer = maybe (throwError ValueWon'tFit) pure . packer

tryParseJSON :: (MonadError DecodeErrorTag m) =>
  (a -> ByteString) -> a -> m Value
tryParseJSON extract v = case eitherDecodeStrict . extract $ v of
  Left err  -> throwError . JSONParseFailed . pack $ err
  Right val -> pure val
