package io.crate.pocs.cli;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;


public class MainClient extends JDBCClient {

    private static final List<Integer> CLIENT_IDS = IntStream.range(0, 21).boxed().collect(toList());
    private static final List<String> SENSOR_IDS = IntStream.range(0, 1000).mapToObj(i -> "sensor_" + i).collect(toList());

    public MainClient(int numValuesInInsert, int numThreads) {
        super(numValuesInInsert, numThreads);
    }

    @Override
    public String tableName() {
        return "doc.sensors";
    }

    @Override
    public String insertPrefix() {
        return "INSERT INTO doc.sensors(client_id, sensor_id, ts, value) VALUES";
    }

    @Override
    public String createTableStmt() {
        return "CREATE TABLE doc.sensors (" +
                "               client_id INTEGER," +
                "               sensor_id TEXT," +
                "               ts TIMESTAMPTZ," +
                "               value DOUBLE PRECISION INDEX OFF," +
                "               PRIMARY KEY (client_id, sensor_id, ts)" +
                "        )" +
                "        CLUSTERED INTO 6 SHARDS" +
                "        WITH (" +
                "               number_of_replicas = 0," +
                "               \"translog.durability\" = 'ASYNC'," +
                "               \"translog.sync_interval\" = 5000," +
                "               refresh_interval = 10000," +
                "               \"store.type\" = 'hybridfs'" +
                "        )";
    }

    @Override
    public String nextBatch() {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        StringBuilder sb = new StringBuilder(insertPrefix());
        for (int i = 0; i < numValuesInInsert(); i++) {
            sb.append("(")
                    .append(randOf(CLIENT_IDS))
                    .append(",")
                    .append(quoted(randOf(SENSOR_IDS)))
                    .append(",")
                    .append(quoted(Instant.now().toString()))
                    .append(",")
                    .append(rand.nextDouble(0.0, 1_000_000.0))
                    .append("), ");
        }
        sb.setLength(sb.length() - 2);
        return sb.toString();
    }

    private static <T> T randOf(List<T> list) {
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }

    private static String quoted(String s) {
        return "'" + s + "'";
    }

    public static void main(String[] args) throws Exception {
        timedInsertRun(5_000, new MainClient(1_000,9), true);
    }
}