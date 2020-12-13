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

import java.io.Closeable;
import java.sql.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public abstract class BaseClient implements Closeable {

    static final Logger LOGGER = LoggerFactory.getLogger(BaseClient.class);

    private final ConnectionFactory conns;
    private final int numThreads;
    private final int numValuesInInsert;
    private final ThreadPoolExecutor executor;

    public BaseClient(ConnectionFactory conns, int numThreads, int numValuesInInsert) {
        this.conns = conns;
        this.numThreads = numThreads;
        this.numValuesInInsert = numValuesInInsert;
        AtomicInteger threadId = new AtomicInteger();
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads, runnable -> {
            Thread thread = threadFactory.newThread(runnable);
            thread.setName(String.format("PostgresClient%s", threadId.getAndIncrement()));
            thread.setDaemon(true);
            return thread;
        });
    }

    public abstract String tableName();

    public abstract String createTableStmt();

    public abstract String dropTableStmt();

    public abstract String insertPrefix();

    public abstract String nextBatch();

    public String uri() {
        return conns.uri();
    }

    public int numValuesInInsert() {
        return numValuesInInsert;
    }

    public int numThreads() {
        return numThreads;
    }

    public void prepareTable() throws SQLException {
        try (Connection conn = conns.newConnection()) {
            try (Statement stmt = conn.createStatement()) {
                LOGGER.info("Dropping table: {}", tableName());
                stmt.execute(dropTableStmt());
                LOGGER.info("Creating table: {}", tableName());
                stmt.execute(createTableStmt());
            }
        }
    }

    public long count() throws SQLException {
        try (Connection conn = conns.newConnection()) {
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
                try (Connection conn = conns.newConnection()) {
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

    @Override
    public void close() {
        executor.shutdownNow();
        try {
            executor.awaitTermination(200L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
