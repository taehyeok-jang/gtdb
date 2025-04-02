## assignment-2

[log_manager.h](./src/include/log/log_manager.h)

[log_manager.cc](./src/log/log_manager.cc)

We implemented logging and recovery mechanism, which is critical for database atomicity and durability. 

For logging, our system outputs WAL(write-ahead log) to stable storage describing the modifications  made by any transaction. For recovery, a simplified verison of the ARIES protocol is implemented, consisting of analysis / redo / undo phases. 
