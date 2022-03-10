# Compression 

### Process 

Grab ~ 1700 messages from the `modern_blocks_json` topic and reinjest them into under different compression modes(above).

Compressors
    - [ ] lz4
    - [ ] zstd
Dimensions to check:
    - from consumer-to-producer (in-memory) production
    - from disk-to-producer production
    - reads speed
Benchmarks:
    - speed
    - memory footrprint
Different subsets of transactions:
Implementations:

    - ts
    - Rust


---------------------------

# Ideas

solana block/transaction types
tx-specific methods




