# JDBC stress insert client

**StressInsertClient** is a JDBC, Java, client that connects to CrateDB using 
the stock Postgres JDBC driver (for your convenience a copy of the jar can be found in 
the ``libs`` folder) and sends random batched inserts at full speed, to populate a table 
(which must exist upfront, and be void of data):

```
CREATE TABLE IF NOT EXISTS doc.sensors (
               client_id INTEGER,
               sensor_id TEXT,
               ts TIMESTAMPTZ,
               value DOUBLE PRECISION INDEX OFF,
               PRIMARY KEY (client_id, sensor_id, ts)
        )
        CLUSTERED INTO 6 SHARDS
        WITH (
               number_of_replicas = 0,
               "translog.durability" = 'ASYNC',
               "translog.sync_interval" = 5000,
               refresh_interval = 10000,
               "store.type" = 'hybridfs'
        );
```

Each insert statement will be of the form:

```
INSERT INTO doc.sensors(client_id, sensor_id, ts, value) VALUES(...), (...), ...
```

with ``numValuesInInsert`` set of values ``(...)`` in each insert. 

The client sends inserts during 3 minutes, at the end of which the number of 
inserts per second, **IPS**, are reported, along with the count(*) and runtime 
in milliseconds.

**NOTES:**

- Do not use JDBC's ``addBatch``, ``executeBatch``, this is very slow, one, 
  possibly two, orders of magnitude slower by comparison. **Instead** create 
  the inserts as described in the code.
- The ``sendBufferSize``, outbound socket buffer size, parameter can be increased
  for better performance.
- The ``numValuesInInsert``, number of values appended to each of the insert 
  statements, parameter can be increased for better performance.

## Run

Either:
 
- ./gradlew jar
- java -jar ./build/libs/crate-pocs-1.0.0-SNAPSHOT.jar

or

- ./gradlew run

