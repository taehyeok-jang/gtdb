# gtdb

gtdb is a toy relational database system built from scratch for educational purposes.  
It implements core database system components such as external sorting, logging and recovery, and concurrency control — motivated by [BuzzDB](https://buzzdb-docs.readthedocs.io/index.html).

# Features

## External Sort

An external merge sort algorithm for sorting large datasets that cannot fit in memory.

- The sorting is done in two phases: 
- First, the input is read in blocks, sorted in-memory, and flushed to temporary files.
- Second, the temporary sorted runs are merged together using a priority queue.

## Logging and Recovery

Inspired by [ARIES](https://en.wikipedia.org/wiki/Algorithms_for_Recovery_and_Isolation_Exploiting_Semantics), gtdb supports **Write-Ahead Logging (WAL)** to ensure durability and atomicity in the event of a crash.

- Log records are written before the actual data pages are flushed (WAL protocol).
- On startup, gtdb performs recovery in two phases:
  - REDO: reapplies committed changes to ensure all effects are persisted.
  - UNDO: rolls back incomplete transactions using the log history.

## Concurrency Control

gtdb implements concurrency control using **two-phase locking (2PL)** and **deadlock detection**.

- Each transaction acquires shared or exclusive locks on records or pages as needed.
- The system enforces lock compatibility and supports lock upgrades.
- Deadlocks are detected via a timeout, and resolved by aborting victim transactions.


# Resources

- [BuzzDB](https://buzzdb-docs.readthedocs.io/index.html)
- [Database System Concepts](https://www.db-book.com/) by Silberschatz, Korth, and Sudarshan.
- [CMU 15-445 Database Systems](https://15445.courses.cs.cmu.edu/)

# License

MIT License
