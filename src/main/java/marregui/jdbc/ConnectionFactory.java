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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory for Jdbc connections.
 */
public class ConnectionFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionFactory.class);

    private static final String DEFAULT_URI_TPT = "jdbc:postgresql://localhost:{}/";
    private static final int[] DEFAULT_PORTS = {5432, 5433, 5434};
    private static final AtomicInteger RR_CONN_IDX = new AtomicInteger();
    private static final Properties DEFAULT_PROPS = new Properties();

    static {
        DEFAULT_PROPS.put("user", "crate");
        DEFAULT_PROPS.put("password", "");
        DEFAULT_PROPS.put("sendBufferSize", 1024 * 1024 * 8); // 8MB
    }


    private final String uriTpt;
    private final String uri;
    private final String[] ports;
    private final Properties connProps;

    public ConnectionFactory() {
        this(DEFAULT_URI_TPT, DEFAULT_PORTS, DEFAULT_PROPS);
    }

    public ConnectionFactory(String uriTpt, int[] ports, Properties connProps) {
        this.uriTpt = Objects.requireNonNull(uriTpt);
        uri = uriTpt.replace("{}", "");
        this.connProps = Objects.requireNonNull(connProps);
        Objects.requireNonNull(ports);
        this.ports = new String[ports.length];
        for (int i = 0; i < ports.length; i++) {
            this.ports[i] = String.valueOf(ports[i]);
        }
    }

    public String uri() {
        return uri;
    }

    public Connection newConnection() throws SQLException {
        return newConnection(false);
    }

    public Connection newConnection(boolean isRoundRobin) throws SQLException {
        for (int i = 0; i < ports.length; i++) {
            int idx = isRoundRobin ? Math.abs(RR_CONN_IDX.getAndIncrement() % ports.length) : i;
            String port = ports[idx];
            String connUri = uriTpt.replace("{}", String.valueOf(port));
            try {
                LOGGER.info("Connecting to: {}", connUri);
                return DriverManager.getConnection(connUri, connProps);
            } catch (SQLException t) {
                LOGGER.warn(" >> failed to connect to: {}", connUri);
                // move on to the next port
            }
        }
        throw new SQLException("Database is unreachable");
    }
}
