┌──────────┐
       │  Client  │
       └────┬─────┘
            │
            │ 1. Stream Bytes (TCP Socket Connection)
            ▼
 ┌──────────────────────┐
 │ Engine Ingress Core  │
 └──────────┬───────────┘
            │
            │ 2. Parse & Frame Payload
            │ 3. Append to Active File Pointer
            ▼
   ┌──────────────────┐
   │  Disk Media      │ ──( 4. Hardware Synchronize: fsync )──► ┌──────────────────────┐
   │  (cache.wal)     │                                         │ Engine Ingress Core  │
   └──────────────────┘                                         └──────────┬───────────┘
                                                                           │
                                                                           │ 5. Lock & Update Map
                                                                           ▼
 ┌──────────┐                                                   ┌──────────────────────┐
 │  Client  │ ◄────────── 6. Push TCP Success Packet ───────────│ In-Memory Cache Index│
 └──────────┘                                                   └──────────────────────┘