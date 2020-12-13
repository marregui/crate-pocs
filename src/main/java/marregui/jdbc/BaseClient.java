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
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public abstract class BaseClient implements Closeable {

    static final Logger LOGGER = LoggerFactory.getLogger(BaseClient.class);

    private final ConnectionFactory conns;
    private final int numInsertAgents;
    private final int batchSize;
    private final ThreadPoolExecutor executor;
    private volatile Connection adminConnection;

    public BaseClient(ConnectionFactory conns, int numInsertAgents, int batchSize) {
        this.conns = Objects.requireNonNull(conns);
        this.numInsertAgents = numInsertAgents;
        this.batchSize = batchSize;
        AtomicInteger threadId = new AtomicInteger();
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        String threadNamePrefix = getClass().getSimpleName();
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numInsertAgents, runnable -> {
            Thread thread = threadFactory.newThread(runnable);
            thread.setName(String.format("%s%s", threadNamePrefix, threadId.getAndIncrement()));
            thread.setDaemon(true);
            return thread;
        });
    }

    public abstract String tableName();

    public abstract String createTableStmt();

    public abstract String insertPrefix();

    public abstract String nextBatch();

    public String uri() {
        return conns.uri();
    }

    public int numInsertAgents() {
        return numInsertAgents;
    }

    public int batchSize() {
        return batchSize;
    }

    public void prepareTable() throws SQLException {
        checkAdminConnection();
        try (Statement stmt = adminConnection.createStatement()) {
            LOGGER.info("Dropping then Creating table: {}", tableName());
            stmt.execute("DROP TABLE IF EXISTS " + tableName());
            stmt.execute(createTableStmt());
        }
    }

    public long count() throws SQLException {
        checkAdminConnection();
        try (Statement stmt = adminConnection.createStatement()) {
            stmt.execute("REFRESH TABLE " + tableName());
            stmt.execute("SELECT count(*) FROM " + tableName());
            ResultSet rs = stmt.getResultSet();
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        throw new IllegalStateException("should never reach here");
    }

    public void stressInsertWhile(Supplier<Boolean> predicate) throws SQLException {
        CountDownLatch completedInserts = new CountDownLatch(numInsertAgents());
        LOGGER.info("Launching {} insert agents", numInsertAgents());
        for (int i = 0; i < numInsertAgents(); i++) {
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

    private synchronized void checkAdminConnection() throws SQLException {
        if (adminConnection == null || adminConnection.isClosed()) {
            adminConnection = conns.newConnection();
        }
    }

    @Override
    public void close() {
        if (adminConnection != null) {
            try {
                adminConnection.close();
            } catch (SQLException e) {
                LOGGER.warn("Failed to close admin connection", e);
            }
        }
        executor.shutdownNow();
        try {
            executor.awaitTermination(200L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
