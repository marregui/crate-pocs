package io.crate.pocs.cli;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;


public class HelloClient extends JDBCClient {

    private static final String URL_TPT = "jdbc:postgresql://presales.bregenz.a1.cratedb.net:{}/";
    private static final Properties PROPS = new Properties();
    static {
        PROPS.put("user", "");
        PROPS.put("password", "");
        PROPS.put("sendBufferSize", 1024 * 1024 * 8);
    }

    public HelloClient(int numValuesInInsert, int numThreads) {
        super(URL_TPT, PROPS, numValuesInInsert, numThreads);
    }

    @Override
    public String tableName() {
        return "doc.hello";
    }

    @Override
    public String insertPrefix() {
        return "INSERT INTO doc.hello(id, avg_value, total_value) VALUES";
    }

    @Override
    public String createTableStmt() {
        return "CREATE TABLE IF NOT EXISTS hello (" +
                "    id INTEGER," +
                "    avg_value DOUBLE PRECISION," +
                "    total_value BIGINT" +
                ")" +
                "CLUSTERED INTO 30 SHARDS " +
                "WITH (" +
                "    \"allocation.max_retries\" = 10," +
                "    column_policy = 'dynamic'," +
                "    number_of_replicas = '0-2'," +
                "    refresh_interval = 1000," +
                "    \"routing.allocation.enable\" = 'all'," +
                "    \"routing.allocation.total_shards_per_node\" = -1," +
                "    \"translog.durability\" = 'REQUEST'," +
                "    \"translog.flush_threshold_size\" = 536870912," +
                "    \"translog.sync_interval\" = 5000," +
                "    \"unassigned.node_left.delayed_timeout\" = 60000," +
                "    \"warmer.enabled\" = true," +
                "    \"write.wait_for_active_shards\" = '1'" +
                ")";
    }

    private static  final AtomicInteger ID = new AtomicInteger();

    @Override
    public String nextBatch() {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        StringBuilder sb = new StringBuilder(insertPrefix());
        for (int i = 0; i < numValuesInInsert(); i++) {
            sb.append("(")
                    .append(ID.getAndIncrement())
                    .append(",")
                    .append(rand.nextDouble(0.0, 1_000_000.0))
                    .append(",")
                    .append(rand.nextInt(0, 1_000_000))
                    .append("), ");
        }
        sb.setLength(sb.length() - 2);
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        timedInsertRun(60_000 * 15, new HelloClient(1_000,9), false);
    }
}