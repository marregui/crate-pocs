package io.crate.pocs.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * CREATE TABLE IF NOT EXISTS sensors (
 *              client_id INTEGER,
 *              sensor_id TEXT,
 *              ts TIMESTAMPTZ,
 *              value DOUBLE PRECISION INDEX OFF,
 *              PRIMARY KEY (client_id, sensor_id, ts))
 *     CLUSTERED INTO 6 SHARDS
 *     WITH (
 *         refresh_interval = 5000,
 *         number_of_replicas = '0'
 *     );
 */
public class InsertValuesJDBCStressClient {

    private static final String INSERT_PREFIX = "INSERT INTO doc.sensors(client_id, sensor_id, ts, value) VALUES";
    private static final List<Integer> CLIENT_IDS = IntStream.range(0, 21).boxed().collect(toList());
    private static final List<String> SENSOR_IDS = IntStream.range(0, 1000).mapToObj(i -> "sensor_" + i).collect(toList());
    private static final Logger LOGGER = LoggerFactory.getLogger(InsertValuesJDBCStressClient.class);


    public static String nextBatch(String insertPrefix, int numValuesInInsert) {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        StringBuilder sb = new StringBuilder(insertPrefix);
        for (int i = 0; i < numValuesInInsert; i++) {
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

    private static final String quoted(String s) {
        return "'" + s + "'";
    }

    /**
     * @param delayMillis delay in milliseconds
     * @return The value is true delayMillis millis after calling the method
     */
    private static AtomicBoolean setTrueOnTimeout(long delayMillis) {
        AtomicBoolean signal = new AtomicBoolean();
        Timer timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                signal.set(true);
                timer.cancel();
                timer.purge();
            }
        }, delayMillis);
        return signal;
    }

    private static <T> T randOf(List<T> list) {
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }

    private static class ConnectionProvider {

        private static String URL_TPT = "jdbc:postgresql://localhost:{}/";
        private static int [] PORTS = { 5432, 5433, 5434 };
        private static Properties PROPS = new Properties();
        static {
            PROPS.put("user", "crate");
            PROPS.put("password", "");
            PROPS.put("sendBufferSize", 1024 * 1024 * 8);
        }

        private final AtomicInteger idx;

        private ConnectionProvider() {
            idx = new AtomicInteger(0);
        }

        private Connection getRoundRobinConnection() {
            for (int i = 0; i < PORTS.length; i++) {
                int port = PORTS[idx.getAndIncrement() % PORTS.length];
                String connUrl = URL_TPT.replace("{}", String.valueOf(port));
                try {
                    LOGGER.info("Connecting with: {}", connUrl);
                    return DriverManager.getConnection(connUrl, PROPS);
                } catch (SQLException t) {
                    LOGGER.info("Could not reach: {}", connUrl);
                }
            }
            throw new RuntimeException("CrateDB is unreachable");
        }
    }

    public static void main(String[] args) throws Exception {

        int numThreads = 9;
        int numValuesInInsert = 1_000;
        long aproxRuntimeMillis = 60_000 * 3;
        int aproxMessageSize = nextBatch(INSERT_PREFIX, numValuesInInsert).length();

        LOGGER.info("Values per insert: {}", numValuesInInsert);
        LOGGER.info("Aprox. message size: {}", aproxMessageSize);
        LOGGER.info("Aprox. runtime millis: {}", aproxRuntimeMillis);
        LOGGER.info("Num. threads: {}", numThreads);

        Instant startTime = Instant.now();
        ExecutorService es = Executors.newFixedThreadPool(numThreads);
        CountDownLatch completedInserts = new CountDownLatch(numThreads);

        // Insert data in parallel
        ConnectionProvider connProvider = new ConnectionProvider();
        for (int i = 0; i < numThreads; i++) {
            int threadId = i + 1;
            es.submit(() -> {
                try (Connection conn = connProvider.getRoundRobinConnection()) {
                    conn.setAutoCommit(false);
                    try (Statement stmt = conn.createStatement()) {
                        LOGGER.info("Thread_{} inserting...", threadId);
                        AtomicBoolean timeoutSignal = setTrueOnTimeout(aproxRuntimeMillis);
                        while (!timeoutSignal.get()) {
                            stmt.execute(nextBatch(INSERT_PREFIX, numValuesInInsert));
                        }
                        conn.commit();
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } finally {
                    completedInserts.countDown();
                    LOGGER.info("Thread_{} completed.", threadId);
                }
            });
        }

        // Wait for all the inserts to be delivered
        completedInserts.await();
        es.shutdown();

        // Show results
        LOGGER.info("Producing results");
        try (Connection conn = connProvider.getRoundRobinConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("REFRESH TABLE sensors");
                long totalMillis = ChronoUnit.MILLIS.between(startTime, Instant.now());
                if (stmt.execute("SELECT count(*) FROM doc.sensors")) {
                    ResultSet rs = stmt.getResultSet();
                    if (rs.next()) {
                        int count = rs.getInt(1);
                        LOGGER.info(
                                "Inserts: {}, millis: {}, IPS: {}",
                                count,
                                totalMillis,
                                Math.round((count * 100_000.0) / totalMillis) / 100.0);
                        return;
                    }
                }
            }
        }
        throw new IllegalStateException("should never reach here");

        // On my MacBook Pro (on a Vanilla Cluster https://github.com/marregui/crate-vanilla-cluster):
        //   Inserts: 889999, millis: 181205, IPS: 4911.56
        //   Inserts: 1945053, millis: 181393, IPS: 10722.87
    }
}