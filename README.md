An ongoing RDBMS Engine written in C++.

The purpose of this project is to create an SQL-like database in modern C++ (CXX26).

The Engine is still under development but supports many features like clustered and non clustered indexes on
1 or multiple columns of a table. It supports table creation, table truncation, column creation, database creation
and many additional features.

This project follows the standards of SQL Server as to how it utilizes the disk I/Os. Futhermore it segments all data in
pages and has a storage manager who is responsible for the allocation deletion and save of a page. Additionaly, it uses an LRU
cache type to remove unused pages when the cache is full and a new page is required to be read from disk.
