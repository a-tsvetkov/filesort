module Lib
    ( fileSort
    ) where

import System.IO
import System.Directory
import Data.Char (isSpace)
import Data.List (intersperse)
import Control.Monad
import Control.Monad.State.Lazy
import Control.Concurrent
import Control.Concurrent.Async
import Data.ByteString.Builder
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.Vector.Unboxed.Mutable as V
import qualified Data.Vector.Algorithms.Intro as I


sortChunk :: V.IOVector Int -> IO String
sortChunk vector = do
  I.sort vector
  (chunkName, chunkHandle) <- openBinaryTempFile "." $ "chunk.tmp"
  writeVector vector chunkHandle
  hClose chunkHandle
  return chunkName

readVector :: V.IOVector Int -> B.ByteString -> IO (V.IOVector Int, B.ByteString)
readVector vector stream = do
  let (ints, rest) = runState (readValues $ V.length vector) stream
  count <- foldM (
    \idx value -> do
      V.unsafeWrite vector idx value
      return (idx + 1)
    ) 0 ints
  let sliced = if count < V.length vector then V.take count vector else vector
  return (sliced, rest)
  where
    readValues :: Int -> State B.ByteString [Int]
    readValues 0 = return []
    readValues n = do
      value <- readInt
      case value of
        Nothing -> return []
        Just v -> (v:) <$> readValues (n-1)

writeVector :: V.IOVector Int -> Handle -> IO ()
writeVector vector handle = do
  let chunkSize = 512
  forM_ (chunks chunkSize [0..V.length vector - 1]) (
    \idxs -> do
      values <- mapM (V.unsafeRead vector) idxs
      B.hPutStr handle $ B.append (writeInts values) $ B.singleton ' '
    )
  where
    chunks :: Int -> [a] -> [[a]]
    chunks _ [] = []
    chunks size lst = let (chunk, rest) = splitAt size lst in chunk:chunks size rest

mergeFiles :: String -> String -> IO String
mergeFiles a b = do
  aHandle <- openBinaryFile a ReadMode
  bHandle <- openBinaryFile b ReadMode
  (tempName, tempHandle) <- openBinaryTempFile "." $ "chunk.tmp"
  aInts <- readInts <$> B.hGetContents aHandle
  bInts <- readInts <$> B.hGetContents bHandle
  B.hPutStr tempHandle $ writeInts $ merge aInts bInts
  hClose tempHandle
  hClose aHandle
  hClose bHandle
  removeFile a
  removeFile b
  return tempName
  where
    merge :: [Int] -> [Int] -> [Int]
    merge [] y = y
    merge x [] = x
    merge x@(xh:xt) y@(yh:yt)
      | xh > yh = yh:merge x yt
      | otherwise = xh:merge y xt

sortWorker :: Int -> MVar B.ByteString -> MVar [String] -> IO ()
sortWorker chunkSize streamMV filesMV = do
  vector <- V.unsafeNew chunkSize
  sortChunks streamMV filesMV vector
  where
    sortChunks :: MVar B.ByteString ->  MVar [String] -> V.IOVector Int -> IO ()
    sortChunks smv fmv vector = do
      stream <- takeMVar smv
      (sliced, rest) <- readVector vector stream
      putMVar smv rest
      if V.length sliced == 0
        then return ()
        else do
          file <- sortChunk sliced
          modifyMVar_ fmv (\files -> return (file:files))
          sortChunks streamMV fmv vector

mergeWorker :: MVar [String] -> IO ()
mergeWorker filesMV = do
  files <- takeMVar filesMV
  case files of
    (a:b:rest) -> do
      putMVar filesMV rest
      result <- mergeFiles a b
      modifyMVar_ filesMV (\f -> return (f ++ [result]))
      mergeWorker filesMV
    _ -> do
      putMVar filesMV files
      return ()

worker :: Int -> MVar B.ByteString -> MVar [String] -> IO ()
worker chunkSize streamMV filesMV = do
  sortWorker chunkSize streamMV filesMV
  mergeWorker filesMV

readInts :: B.ByteString -> [Int]
readInts = evalState readAll
  where
    readAll :: State B.ByteString [Int]
    readAll = do
      value <- readInt
      case value of
        Just v -> (v:) <$> readAll
        Nothing -> return []

readInt :: State B.ByteString (Maybe Int)
readInt = do
  bs <- get
  case B.readInt $ B.dropWhile (isSpace) bs of
    Just (value, rest) -> do
      put rest
      return (Just value)
    Nothing -> return Nothing


writeInts :: [Int] -> B.ByteString
writeInts = toLazyByteString . listBuilder
  where
    listBuilder :: [Int] -> Builder
    listBuilder lst = mconcat $ intersperse (charUtf8 ' ') $ map intDec lst


fileSort :: Int -> String -> String -> IO ()
fileSort chunkSize inFile outFile = do
  cores <- getNumCapabilities
  finalFile <- withBinaryFile inFile ReadMode (
    \handle -> do
      results <- newMVar []
      intStream <- B.hGetContents handle >>= newMVar
      asyncs <- replicateM (cores-1) $ asyncBound $ worker chunkSize intStream results
      worker chunkSize intStream results
      mapM_ wait asyncs
      (result:_) <- takeMVar results
      return result
    )
  renameFile finalFile outFile
