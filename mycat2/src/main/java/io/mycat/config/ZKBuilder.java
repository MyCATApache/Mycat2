/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.config;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.client.ConnectStringParser;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.stream.Collectors;

public class ZKBuilder {
    private static final int DEFAULT_CONNECTION_TIMEOUT_SECONDS = 15;
    private final String connectionString;
    private String root = "";
    private RetryPolicy retryPolicy = new RetryOneTime(1000);
    private Duration duration = Duration.ofSeconds(DEFAULT_CONNECTION_TIMEOUT_SECONDS);

    public ZKBuilder(String connectionString) {
        this.connectionString = connectionString;
    }

    public ZKBuilder withRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
    }

    public ZKBuilder withRoot(String root) {
        if (root == null) {
            this.root = "";
        } else if (root.endsWith("/")) {
            this.root = root.substring(0, root.length() - 1);
        } else {
            this.root = root;
        }
        if (!this.root.isEmpty() && !this.root.startsWith("/")) {
            throw new IllegalArgumentException("Root path should start with \"/\"");
        }
        return this;
    }

    public ZKBuilder withConnectionTimeout(Duration duration) {
        this.duration = duration;
        return this;
    }

    public CuratorFramework build() throws Exception {
        return newCuratorFrameworkClient(this, connectionString);
    }

    public static CuratorFramework newCuratorFrameworkClient(ZKBuilder builder, String connectionString) throws Exception {
        ConnectStringParser connectStringParser = new ConnectStringParser(connectionString);
        if (connectStringParser.getChrootPath() != null) {
            final String connectionStringForChrootCreation = connectStringParser.getServerAddresses().stream().map(InetSocketAddress::toString).collect(Collectors.joining(","));
            try (final CuratorFramework clientForChrootCreation = newCuratorFrameworkClient(builder, connectionStringForChrootCreation)) {
                clientForChrootCreation.start();
                if (!clientForChrootCreation.getZookeeperClient().blockUntilConnectedOrTimedOut()) {
                    throw new IOException("Did not connect in time: " + clientForChrootCreation.getZookeeperClient().getConnectionTimeoutMs() + " ms");
                }
                clientForChrootCreation.createContainers(connectStringParser.getChrootPath());
            }
        }

        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(connectionString)
                .retryPolicy(builder.retryPolicy)
                .connectionTimeoutMs((int) builder.duration.toMillis())
                .build();
         curatorFramework.start();
        return curatorFramework;
    }
}