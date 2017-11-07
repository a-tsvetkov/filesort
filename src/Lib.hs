module Lib
    ( fileSort
    ) where

import System.IO
import System.Directory
import Data.Maybe
import Control.Monad
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.Vector.Unboxed.Mutable as V
import qualified Data.Vector.Algorithms.Intro as I


splitFile :: Int -> String -> IO [String]
splitFile chunkSize file = do
  vector <- V.unsafeNew chunkSize
  withBinaryFile file ReadMode (\handle -> readInts <$> B.hGetContents handle >>= makeChunks vector)

makeChunks :: V.IOVector Int -> [Int] -> IO [String]
makeChunks _ [] = return []
makeChunks vector stream = do
  let (chunk, rest) = splitAt (V.length vector) stream
  chunkName <- makeChunk vector chunk
  (chunkName:) <$> makeChunks vector rest

makeChunk :: V.IOVector Int -> [Int] -> IO (String)
makeChunk vector list = do
  filled <- readVector vector list
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

mergeChunks :: [String] -> IO String
mergeChunks [x] = return x
mergeChunks x = mergeOnce x >>= mergeChunks
  where
    mergeOnce :: [String] -> IO [String]
    mergeOnce (a:b:rest) = do
      newFile <- mergeFiles a b
      (newFile:) <$> mergeOnce rest
    mergeOnce a = return a

readInts :: B.ByteString -> [Int]
readInts = mapMaybe (fmap (fst) . B.readInt) . B.words

writeInts :: [Int] -> B.ByteString
writeInts = B.unwords . map (B.pack . show)

fileSort :: Int -> String -> String -> IO ()
fileSort chunkSize inFile outFile = do
  sortedChunks <- splitFile chunkSize inFile
  finalFile <- mergeChunks sortedChunks
  renameFile finalFile outFile
