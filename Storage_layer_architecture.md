To bypass file-system tree adjustments and indexing penalties, the storage subsystem tracks events linearly. Appending bytes to the end of a contiguous binary stream bounds physical disk routines to an O(1) temporal constraint.

Binary Wire Framing Layout
Every entry written to cache.wal follows a packed binary wire formatting layout to ensure precise offset navigation during crash-recovery sweeps:
0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                 Payload Length (4 Bytes / uint32)             |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
+                       Timestamp (8 Bytes / int64)             +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      Raw Message Payload                      |
|                        (Variable Length)                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

Field Specifications:
Payload Length (4 Bytes / uint32): Dictates the exact reading boundary of the payload array immediately following the fixed header frame. Max theoretical size per entry is 4 GB.

Timestamp (8 Bytes / int64): Nanosecond or millisecond UNIX epoch tracking token generated at ingestion time.

Raw Payload (Variable Length): The raw byte slice intercepted directly from the client socket.