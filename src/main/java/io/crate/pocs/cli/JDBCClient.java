package io.crate.pocs.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

// https://github.com/marregui/crate-vanilla-cluster
public abstract class JDBCClient implements Closeable {

    static final Logger LOGGER = LoggerFactory.getLogger(JDBCClient.class);

    private static final String DEFAULT_URL_TPT = "jdbc:postgresql://localhost:{}/";
    private static final int[] PORTS = {5432, 5433, 5434};
    private static final Properties DEFAULT_PROPS = new Properties();
    static {
        DEFAULT_PROPS.put("user", "crate");
        DEFAULT_PROPS.put("password", "");
        DEFAULT_PROPS.put("sendBufferSize", 1024 * 1024 * 8);
    }

    public static void timedInsertRun(int millis, JDBCClient client, boolean cleanTable) throws SQLException {
        try(client) {
            LOGGER.info("Insert activity will be {} millis with {}", millis, client.url());
            long currentCount = 0L;
            if (cleanTable) {
                client.prepareTable();
            } else {
                currentCount = client.count();
            }
            Instant startTime = Instant.now();
            AtomicBoolean timeoutSignal = setTrueOnTimeout(millis);
            client.insertWhile(() -> !timeoutSignal.get());
            client.logResults(startTime, currentCount);
        }
    }

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

    private final String urlTpt;
    private final Properties connProps;
    private final int numValuesInInsert;
    private final int numThreads;
    private final ThreadPoolExecutor executor;

    public JDBCClient(int numValuesInInsert, int numThreads) {
        this(DEFAULT_URL_TPT, DEFAULT_PROPS, numValuesInInsert, numThreads);
    }

    public JDBCClient(String urlTpt, Properties connProps, int numValuesInInsert, int numThreads) {
        this.urlTpt = urlTpt;
        this.connProps = connProps;
        this.numValuesInInsert = numValuesInInsert;
        this.numThreads = numThreads;
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
    }

    public abstract String tableName();

    public abstract String createTableStmt();

    public abstract String nextBatch();

    public abstract String insertPrefix();

    public int numValuesInInsert() {
        return numValuesInInsert;
    }

    public int numThreads() {
        return numThreads;
    }

    public String url() {
        return urlTpt.replace("{}", "");
    }

    private static final AtomicInteger RR_CONN_IDX = new AtomicInteger();

    private Connection getRoundRobinConnection() {
        for (int i = 0; i < PORTS.length; i++) {
            int port = PORTS[Math.abs(RR_CONN_IDX.getAndIncrement() % PORTS.length)];
            String connUrl = urlTpt.replace("{}", String.valueOf(port));
            try {
                LOGGER.debug("Connecting with: {} ({})", connUrl, connProps);
                return DriverManager.getConnection(connUrl, connProps);
            } catch (SQLException t) {
                // move on to the next port
            }
        }
        throw new RuntimeException("CrateDB is unreachable");
    }

    public void prepareTable() throws SQLException {
        try (Connection conn = getRoundRobinConnection()) {
            try (Statement stmt = conn.createStatement()) {
                LOGGER.info("Dropping table: {}", tableName());
                stmt.execute("DROP TABLE IF EXISTS " + tableName());
                LOGGER.info("Creating table: {}", tableName());
                stmt.execute(createTableStmt());
            }
        }
    }

    public long count() throws SQLException {
        try (Connection conn = getRoundRobinConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("REFRESH TABLE " + tableName());
                stmt.execute("SELECT count(*) FROM " + tableName());
                ResultSet rs = stmt.getResultSet();
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }
        throw new IllegalStateException("should never reach here");
    }

    public void insertWhile(Supplier<Boolean> predicate) throws SQLException {
        CountDownLatch completedInserts = new CountDownLatch(numThreads());
        LOGGER.info("Launching {} insert threads", numThreads());
        for (int i = 0; i < numThreads(); i++) {
            executor.submit(() -> {
                try (Connection conn = getRoundRobinConnection()) {
                    conn.setAutoCommit(false);
                    try (Statement stmt = conn.createStatement()) {
                        while (predicate.get()) {
                            stmt.execute(nextBatch());
                        }
                        conn.commit();
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } finally {
                    completedInserts.countDown();
                }
            });
        }
        try {
            completedInserts.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted while waiting at insertWhile");
        }
    }

    public void logResults(Instant startTime, long preRunCount) throws SQLException {
        LOGGER.info("Results for table: {}", tableName());
        LOGGER.info("   Host: {}", url());
        LOGGER.info("   Insert prefix: {}", insertPrefix());
        LOGGER.info("   Values per insert: {}", numValuesInInsert());
        LOGGER.info("   Aprox. insert size: {}", nextBatch().length());
        LOGGER.info("   Num. threads: {}", numThreads());
        LOGGER.info("   Pre run count: {}", preRunCount);
        long count = count() ;
        LOGGER.info("   Post run count: {}", count);
        count = count - preRunCount;
        long totalMillis = ChronoUnit.MILLIS.between(startTime, Instant.now());
        LOGGER.info(">> Inserts: {}, Elapsed (ms): {}, IPS: {}",
                count,
                totalMillis,
                Math.round((count * 100_000.0) / totalMillis) / 100.0);
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}