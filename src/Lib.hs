module Lib
    ( fileSort
    ) where

import System.IO
import System.Directory
import Data.Char (isSpace)
import Data.List (intersperse)
import Data.Maybe (fromMaybe, catMaybes)
import Data.ByteString.Builder
import Control.Monad
import Control.Monad.State.Lazy
import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
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

readVector :: V.IOVector Int -> TVar B.ByteString -> IO (V.IOVector Int)
readVector vector stream = do
  ints <- atomically (
    do
      st <- readTVar stream
      let (parsed, rest) = runState (readValues $ V.length vector) st
      writeTVar stream rest
      return parsed
    )
  count <- foldM (
    \idx value -> do
      V.unsafeWrite vector idx value
      return (idx + 1)
    ) 0 ints
  let sliced = if count < V.length vector then V.take count vector else vector
  return sliced
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
  forM_ (chunks chunkSize [0..V.length vector - 1]) (
    \idxs -> do
      values <- mapM (V.unsafeRead vector) idxs
      B.hPutStr handle $ B.append (writeInts values) $ B.singleton ' '
    )
  where
    chunkSize = 512

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

sortWorker :: Int -> TVar B.ByteString -> TQueue String -> IO ()
sortWorker chunkSize stream files = do
  vector <- V.unsafeNew chunkSize
  sortChunks stream files vector
  where
    sortChunks :: TVar B.ByteString ->  TQueue String -> V.IOVector Int -> IO ()
    sortChunks st fs vc = do
      sliced <- readVector vc st
      if V.length sliced == 0
        then return ()
        else do
          file <- sortChunk sliced
          atomically $ writeTQueue files file
          sortChunks st fs vc

mergeWorker :: TQueue String -> IO ()
mergeWorker filesQueue = do
  files <- atomically $ replicateM 2 $ tryReadTQueue filesQueue
  case catMaybes files of
    (a:b:_) -> do
      result <- mergeFiles a b
      atomically $ writeTQueue filesQueue result
      mergeWorker filesQueue
    (a:[]) -> atomically $ writeTQueue filesQueue a
    [] -> return ()

worker :: Int -> TVar B.ByteString -> TQueue String -> IO ()
worker chunkSize stream files = do
  sortWorker chunkSize stream files
  mergeWorker files

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
  let result = B.readInt $ B.dropWhile (isSpace) bs
  put $ fromMaybe bs (snd <$> result)
  return (fst <$> result)

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
      results <- newTQueueIO
      intStream <- newTVarIO =<< B.hGetContents handle
      asyncs <- replicateM (cores-1) $ asyncBound $ worker chunkSize intStream results
      worker chunkSize intStream results
      mapM_ wait asyncs
      result <- atomically $ readTQueue results
      return result
    )
  renameFile finalFile outFile
