module Lib
    ( fileSort
    ) where

import System.IO
import System.Directory
import Data.Maybe
import Control.Monad
import Control.Concurrent
import Control.Concurrent.Async
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.Vector.Unboxed.Mutable as V
import qualified Data.Vector.Algorithms.Intro as I


sortChunk :: V.IOVector Int -> [Int] -> IO String
sortChunk vector chunk = do
  filled <- readVector vector chunk
  I.sort filled
  (chunkName, chunkHandle) <- openBinaryTempFile "." $ "chunk.tmp"
  writeVector filled chunkHandle
  hClose chunkHandle
  return chunkName

readVector :: V.IOVector Int -> [Int] -> IO (V.IOVector Int)
readVector vector list = do
  count <- foldM (
    \idx value -> do
      V.unsafeWrite vector idx value
      return (idx + 1)
    ) 0 list
  let sliced = if count < V.length vector then V.take count vector else vector
  return sliced

writeVector :: V.IOVector Int -> Handle -> IO ()
writeVector vector handle =
  forM_ [0..(V.length vector - 1)] (
  \index -> do
    item <- V.unsafeRead vector index
    hPutStr handle $ show item ++ " "
  )

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

sortWorker :: Int -> MVar [Int] -> MVar [String] -> IO ()
sortWorker chunkSize streamMV filesMV = do
  vector <- V.unsafeNew chunkSize
  sortChunks streamMV filesMV vector
  where
    sortChunks :: MVar [Int] ->  MVar [String] -> V.IOVector Int -> IO ()
    sortChunks smv fmv vector = do
      stream <- takeMVar smv
      let (chunk, rest) = splitAt (V.length vector) stream
      putMVar smv rest
      case chunk of
        [] -> return ()
        c -> do
          file <- sortChunk vector c
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

worker :: Int -> MVar [Int] -> MVar [String] -> IO ()
worker chunkSize streamMV filesMV = do
  sortWorker chunkSize streamMV filesMV
  mergeWorker filesMV

readInts :: B.ByteString -> [Int]
readInts = mapMaybe (fmap (fst) . B.readInt) . B.words

writeInts :: [Int] -> B.ByteString
writeInts = B.unwords . map (B.pack . show)

fileSort :: Int -> String -> String -> IO ()
fileSort chunkSize inFile outFile = do
  cores <- getNumCapabilities
  finalFile <- withBinaryFile inFile ReadMode (
    \handle -> do
      results <- newMVar []
      intStream <- readInts <$> B.hGetContents handle >>= newMVar
      asyncs <- replicateM (cores-1) $ asyncBound $ worker chunkSize intStream results
      worker chunkSize intStream results
      mapM_ wait asyncs
      (result:_) <- takeMVar results
      return result
    )
  renameFile finalFile outFile
