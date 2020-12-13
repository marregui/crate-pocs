/*
 * Licensed to Miguel Arregui ("marregui") under one or more contributor
 * license agreements. See the LICENSE file distributed with this work
 * for additional information regarding copyright ownership. You may
 * obtain a copy at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * Copyright 2020, Miguel Arregui a.k.a. marregui
 */

package marregui.jdbc;

import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import java.util.stream.Collectors;

public class CrateDBStressInsertClient extends BaseClient {

    private static final List<Integer> CLIENT_IDS = IntStream
            .range(0, 21)
            .boxed()
            .collect(Collectors.toList());
    private static final List<String> SENSOR_IDS = IntStream
            .range(0, 1000)
            .mapToObj(i -> "sensor_" + i)
            .collect(Collectors.toList());


    public CrateDBStressInsertClient(int numInsertAgents, int batchSize) {
        super(new ConnectionFactory(), numInsertAgents, batchSize);
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
                "        CLUSTERED INTO 1 SHARDS" +
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
        for (int i = 0; i < batchSize(); i++) {
            sb.append("(")
                    .append(randOf(rand, CLIENT_IDS))
                    .append(",")
                    .append(quoted(randOf(rand, SENSOR_IDS)))
                    .append(",")
                    .append(quoted(Instant.now().toString()))
                    .append(",")
                    .append(rand.nextDouble(0.0, 1_000_000.0))
                    .append("), ");
        }
        sb.setLength(sb.length() - 2);
        return sb.toString();
    }

    public static void logResults(BaseClient client, Instant startTime, long preRunCount) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("Results for table: ").append(client.tableName()).append("\n");
        sb.append("   Host: ").append(client.uri()).append("\n");
        sb.append("   Insert prefix: ").append(client.insertPrefix()).append("\n");
        sb.append("   Values per insert: ").append(client.batchSize()).append("\n");
        sb.append("   Aprox. insert size: ").append(client.nextBatch().length()).append("\n");
        sb.append("   Num. threads: ").append(client.numInsertAgents()).append("\n");
        sb.append("   Pre run count: ").append(preRunCount).append("\n");
        long count = client.count();
        sb.append("   Post run count: ").append(count).append("\n");
        count = count - preRunCount;
        long totalMillis = ChronoUnit.MILLIS.between(startTime, Instant.now());
        sb.append(">> Inserts: ").append(count)
                .append(", Elapsed (ms): ").append(totalMillis)
                .append(", IPS: ").append(Math.round((count * 100_000.0) / totalMillis) / 100.0);
        LOGGER.info(sb.toString());
    }

    private static <T> T randOf(ThreadLocalRandom rand, List<T> list) {
        return list.get(rand.nextInt(list.size()));
    }

    private static String quoted(String s) {
        return "'" + s + "'";
    }

    private static AtomicBoolean trueOnTimeoutSignal(long delayMillis) {
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

    public static void main(String[] args) throws Exception {
        int numInsertAgents = 10;
        int batchSize = 200;
        long duration = 7_000L;
        try (CrateDBStressInsertClient client = new CrateDBStressInsertClient(numInsertAgents, batchSize)) {
            LOGGER.info("Insert during {} millis into {}", duration, client.uri());
            long preRunCount = 0L;
            client.prepareTable();
            Instant startTime = Instant.now();
            AtomicBoolean timeoutSignal = trueOnTimeoutSignal(duration);
            client.stressInsertWhile(() -> !timeoutSignal.get());
            logResults(client, startTime, preRunCount);
        }
    }
}