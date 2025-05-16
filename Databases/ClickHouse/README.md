# Installation on MacOS

## Method 1: `brew install`

Reference: https://clickhouse.com/docs/install/macOS

```bash
$ brew install --cask clickhouse
$ clickhouse
```
```
ClickHouse local version 25.4.4.25 (official build).

:)
```

## Method 2: Use binary (Preferred)

Reference: https://clickhouse.com/docs/getting-started/quick-start

```bash
# Download ClickHouse
$ curl https://clickhouse.com/ | sh

# Successfully downloaded the ClickHouse binary, you can run it as:
$ ./clickhouse

# You can also install it:
$ sudo ./clickhouse install

# Start the server
$ ./clickhouse server

# Start the client
$ ./clickhouse client
```

```sql
-- Create a table
CREATE TABLE my_first_table
(
    user_id UInt32,
    message String,
    timestamp DateTime,
    metric Float32
)
ENGINE = MergeTree
PRIMARY KEY (user_id, timestamp)

-- Insert data
INSERT INTO my_first_table (user_id, message, timestamp, metric) VALUES
    (101, 'Hello, ClickHouse!',                                 now(),       -1.0    ),
    (102, 'Insert a lot of rows per batch',                     yesterday(), 1.41421 ),
    (102, 'Sort your data based on your commonly-used queries', today(),     2.718   ),
    (101, 'Granules are the smallest chunks of data read',      now() + 5,   3.14159 )

-- Query your new table
SELECT *
FROM my_first_table
ORDER BY timestamp
```