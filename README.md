# Crate POCs

**InsertValuesStressTest** is a JDBC client that connects to CrateDB using the stock 
Postgres jdbc driver (for your convenience a copy of the jar can be found in the libs 
folder) and sends random batched inserts at full speed, to populate a table (which 
must exist upfront):

```
CREATE TABLE IF NOT EXISTS sensors (
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

The test is run for 30 seconds, which I recommend you change to 5 minutes 
``aproxRuntimeMillis`` -> 60_000 * 5. At the end of the run, the number of 
inserts per second is reported.

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


