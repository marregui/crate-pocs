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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ClientTools {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientTools.class);

    public static void timedInsertRun(BaseClient client, int millis, boolean cleanTable) throws SQLException {
        try (client) {
            LOGGER.info("Insert during {} millis into {}", millis, client.uri());
            long currentCount = 0L;
            if (cleanTable) {
                client.prepareTable();
            } else {
                currentCount = client.count();
            }
            Instant startTime = Instant.now();
            AtomicBoolean timeoutSignal = trueOnTimeoutSignal(millis);
            client.insertWhile(() -> !timeoutSignal.get());
            logResults(client, startTime, currentCount);
        }
    }

    public static void logResults(BaseClient client, Instant startTime, long preRunCount) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("Results for table: ").append(client.tableName()).append("\n");
        sb.append("   Host: ").append(client.uri()).append("\n");
        sb.append("   Insert prefix: ").append(client.insertPrefix()).append("\n");
        sb.append("   Values per insert: ").append(client.numValuesInInsert()).append("\n");
        sb.append("   Aprox. insert size: ").append(client.nextBatch().length()).append("\n");
        sb.append("   Num. threads: ").append(client.numThreads()).append("\n");
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
}