# Crate POCs

**InsertValuesJDBCStressClient** is a JDBC client that connects to CrateDB using the 
stock Postgres jdbc driver (for your convenience a copy of the jar can be found in the 
libs folder) and sends random batched inserts at full speed, to populate a table (which 
must exist upfront, and be void of data):

```
CREATE TABLE IF NOT EXISTS doc.sensors (
               client_id INTEGER,
               sensor_id TEXT,
               ts TIMESTAMPTZ,
               value DOUBLE PRECISION INDEX OFF,
               PRIMARY KEY (client_id, sensor_id, ts));  
```

Each insert statement will be of the form:

```
INSERT INTO doc.sensors(client_id, sensor_id, ts, value) VALUES(...), (...), ...
```

with ``numValuesInInsert`` set of values ``(...)`` in each insert. 

The client sends inserts during 3 minutes, at the end of which the number of 
inserts per second, IPS, are reported, along with the count(*) and time in millis.

**NOTES:**

- Do not use addBatch, it is slow, instead create the inserts as described in the code
- The outbout socket buffer size matters, the more, the better
- The number of values appended to the insert statements matters, the more, the better
- Use only one thread, otherwise they compete and make each other sleep

## Run

Either
 
- ./gradlew jar
- java -jar ./build/libs/crate-pocs-1.0.0-SNAPSHOT.jar

or

- ./gradlew run


