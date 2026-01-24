@0xc1b9c8de8a1e4b2f;

interface LogService {
  append @0 (key :Data, value :Data) -> (entryLen :UInt32, valueOffset :UInt64, valueLen :UInt32);
  readValue @1 (offset :UInt64, len :UInt32) -> (data :Data);
  flush @2 () -> ();
  currentSize @3 () -> (size :UInt64);
}
