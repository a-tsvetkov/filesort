module Main where

import System.Environment
import Data.Maybe
import Data.List (uncons)
import Lib

main :: IO ()
main = do
  (inFile:outFile:args) <- getArgs
  let chunkSize = read $ fromMaybe "10000" $ fst <$> uncons args :: Int
  fileSort chunkSize inFile outFile
