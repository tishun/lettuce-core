/*
 * Copyright 2011-2025, Redis Ltd. and Contributors
 * All rights reserved.
 *
 * Licensed under the MIT License.
 *
 * This file contains contributions from third-party contributors
 * licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lettuce.core.multidb;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.ConnectionFuture;
import io.lettuce.core.RedisChannelHandler;
import io.lettuce.core.RedisChannelWriter;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.CommandExpiryWriter;
import io.lettuce.core.protocol.PushHandler;
import io.lettuce.core.resource.ClientResources;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * A Redis client that manages multiple database connections and allows switching between them. All database connections are
 * established upfront. Only one connection is active at any given time, and switching databases will cancel all in-flight
 * commands on the previous connection.
 * 
 * <p>
 * This client extends {@link AbstractRedisClient} and manages multiple {@link RedisClient} instances internally, one for each
 * database.
 * </p>
 * 
 * @author Tihomir Mateev
 * @since 6.6
 */
public class MultiDatabaseRedisClient extends AbstractRedisClient {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MultiDatabaseRedisClient.class);

    private final Map<Integer, RedisClient> clients = new ConcurrentHashMap<>();

    private final Map<Integer, RedisURI> databaseURIs;

    private final int initialDatabase;

    /**
     * Create a new {@link MultiDatabaseRedisClient} with the given database URIs.
     *
     * @param databaseURIs map of database index to Redis URIs, must not be {@code null} or empty
     * @param initialDatabase the initial active database index
     */
    protected MultiDatabaseRedisClient(Map<Integer, RedisURI> databaseURIs, int initialDatabase) {
        this(null, databaseURIs, initialDatabase);
    }

    /**
     * Create a new {@link MultiDatabaseRedisClient} with the given client resources and database URIs.
     *
     * @param clientResources the client resources, can be {@code null} to use default resources
     * @param databaseURIs map of database index to Redis URIs, must not be {@code null} or empty
     * @param initialDatabase the initial active database index
     */
    protected MultiDatabaseRedisClient(ClientResources clientResources, Map<Integer, RedisURI> databaseURIs,
            int initialDatabase) {

        super(clientResources);

        LettuceAssert.notNull(databaseURIs, "Database URIs must not be null");
        LettuceAssert.isTrue(!databaseURIs.isEmpty(), "Database URIs must not be empty");
        LettuceAssert.isTrue(databaseURIs.containsKey(initialDatabase),
                "Database URIs must contain the initial database: " + initialDatabase);

        this.databaseURIs = new HashMap<>(databaseURIs);
        this.initialDatabase = initialDatabase;

        // Create RedisClient instances for each database
        for (Map.Entry<Integer, RedisURI> entry : databaseURIs.entrySet()) {
            int dbIndex = entry.getKey();
            RedisURI uri = entry.getValue();

            RedisClient client = RedisClient.create(getResources(), uri);

            // Copy client options if set
            if (getOptions() != null) {
                client.setOptions((ClientOptions) getOptions());
            }

            clients.put(dbIndex, client);

            if (logger.isDebugEnabled()) {
                logger.debug("Created RedisClient for database {}: {}", dbIndex, uri);
            }
        }
    }

    /**
     * Create a new {@link MultiDatabaseRedisClient} with the given database URIs.
     *
     * @param databaseURIs map of database index to Redis URIs, must not be {@code null} or empty
     * @param initialDatabase the initial active database index
     * @return a new {@link MultiDatabaseRedisClient}
     */
    public static MultiDatabaseRedisClient create(Map<Integer, RedisURI> databaseURIs, int initialDatabase) {
        return new MultiDatabaseRedisClient(databaseURIs, initialDatabase);
    }

    /**
     * Create a new {@link MultiDatabaseRedisClient} with the given client resources and database URIs.
     *
     * @param clientResources the client resources, must not be {@code null}
     * @param databaseURIs map of database index to Redis URIs, must not be {@code null} or empty
     * @param initialDatabase the initial active database index
     * @return a new {@link MultiDatabaseRedisClient}
     */
    public static MultiDatabaseRedisClient create(ClientResources clientResources, Map<Integer, RedisURI> databaseURIs,
            int initialDatabase) {

        LettuceAssert.notNull(clientResources, "ClientResources must not be null");
        return new MultiDatabaseRedisClient(clientResources, databaseURIs, initialDatabase);
    }

    /**
     * Connect to Redis and return a switchable connection that can switch between databases. All database connections are
     * established upfront.
     *
     * @return a new switchable connection
     */
    public SwitchableRedisConnection<String, String> connect() {
        return connect(StringCodec.UTF8);
    }

    /**
     * Connect to Redis using the supplied {@link RedisCodec} and return a switchable connection. All database connections are
     * established upfront.
     *
     * @param codec the codec to use for encoding/decoding keys and values, must not be {@code null}
     * @param <K> Key type
     * @param <V> Value type
     * @return a new switchable connection
     */
    public <K, V> SwitchableRedisConnection<K, V> connect(RedisCodec<K, V> codec) {
        return getConnection(connectAsync(codec));
    }

    /**
     * Connect asynchronously to Redis and return a switchable connection. All database connections are established upfront.
     *
     * @return a {@link ConnectionFuture} for the switchable connection
     */
    public ConnectionFuture<SwitchableRedisConnection<String, String>> connectAsync() {
        return connectAsync(StringCodec.UTF8);
    }

    /**
     * Connect asynchronously to Redis using the supplied {@link RedisCodec}. All database connections are established upfront.
     *
     * @param codec the codec to use for encoding/decoding keys and values, must not be {@code null}
     * @param <K> Key type
     * @param <V> Value type
     * @return a {@link ConnectionFuture} for the switchable connection
     */
    public <K, V> ConnectionFuture<SwitchableRedisConnection<K, V>> connectAsync(RedisCodec<K, V> codec) {

        LettuceAssert.notNull(codec, "RedisCodec must not be null");

        if (logger.isDebugEnabled()) {
            logger.debug("Connecting to multiple databases: {}", databaseURIs.keySet());
        }

        // Connect to all databases
        Map<Integer, ConnectionFuture<StatefulRedisConnection<K, V>>> connectionFutures = new HashMap<>();

        for (Map.Entry<Integer, RedisClient> entry : clients.entrySet()) {
            int dbIndex = entry.getKey();
            RedisClient client = entry.getValue();
            RedisURI uri = databaseURIs.get(dbIndex);

            ConnectionFuture<StatefulRedisConnection<K, V>> future = client.connectAsync(codec, uri);
            connectionFutures.put(dbIndex, future);
        }

        // Wait for all connections to complete
        CompletableFuture<?>[] allFutures = connectionFutures.values().stream()
                .map(cf -> (CompletableFuture<?>) cf.toCompletableFuture()).toArray(CompletableFuture[]::new);

        CompletableFuture<SwitchableRedisConnection<K, V>> resultFuture = CompletableFuture.allOf(allFutures).thenApply(v -> {

            // Extract channel writers from all connections
            Map<Integer, RedisChannelWriter> writers = new HashMap<>();
            Map<Integer, StatefulRedisConnection<K, V>> connections = new HashMap<>();

            for (Map.Entry<Integer, ConnectionFuture<StatefulRedisConnection<K, V>>> entry : connectionFutures.entrySet()) {
                int dbIndex = entry.getKey();
                StatefulRedisConnection<K, V> connection = entry.getValue().join();

                connections.put(dbIndex, connection);

                // Extract the channel writer from the connection
                if (connection instanceof RedisChannelHandler) {
                    RedisChannelWriter writer = ((RedisChannelHandler<K, V>) connection).getChannelWriter();
                    writers.put(dbIndex, writer);
                } else {
                    throw new IllegalStateException("Connection does not expose channel writer: " + connection.getClass());
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Connected to database {}", dbIndex);
                }
            }

            // Get timeout from the initial database URI
            Duration timeout = databaseURIs.get(initialDatabase).getTimeout();

            // Create a PushHandler that delegates to the active database's connection
            // We need to unwrap the channel writer to get to the underlying DefaultEndpoint
            PushHandler pushHandler = unwrapPushHandler(writers.get(initialDatabase));

            // Create the switchable connection
            SwitchableRedisConnection<K, V> switchableConnection = new SwitchableRedisConnection<>(writers, initialDatabase,
                    pushHandler, codec, timeout);

            // Set client options on the switchable connection
            if (getOptions() != null) {
                switchableConnection.setOptions(getOptions());
            }

            // Track the connection for cleanup
            closeableResources.add(switchableConnection);

            // Track all underlying connections for cleanup
            for (StatefulRedisConnection<K, V> connection : connections.values()) {
                if (connection instanceof Closeable) {
                    closeableResources.add((Closeable) connection);
                }
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Created switchable connection with initial database {}", initialDatabase);
            }

            return switchableConnection;
        });

        // Get the remote address from the initial database connection
        SocketAddress remoteAddress = connectionFutures.get(initialDatabase).getRemoteAddress();

        return ConnectionFuture.from(remoteAddress, resultFuture);
    }

    @Override
    public void shutdown() {
        shutdown(2, 15, TimeUnit.SECONDS);
    }

    @Override
    public void shutdown(long quietPeriod, long timeout, TimeUnit timeUnit) {

        // Close all clients
        List<CompletableFuture<Void>> shutdownFutures = new ArrayList<>();

        for (RedisClient client : clients.values()) {
            try {
                CompletableFuture<Void> future = CompletableFuture
                        .runAsync(() -> client.shutdown(quietPeriod, timeout, timeUnit));
                shutdownFutures.add(future);
            } catch (Exception e) {
                logger.warn("Error shutting down client", e);
            }
        }

        // Wait for all clients to shut down
        CompletableFuture.allOf(shutdownFutures.toArray(new CompletableFuture[0])).join();

        clients.clear();

        // Call parent shutdown
        super.shutdown(quietPeriod, timeout, timeUnit);
    }

    /**
     * Unwrap the channel writer to get the underlying PushHandler. The channel writer may be wrapped in CommandExpiryWriter or
     * MaintenanceAwareExpiryWriter, so we need to unwrap it to get to the DefaultEndpoint.
     *
     * @param writer the channel writer to unwrap
     * @return the PushHandler
     * @throws IllegalStateException if the PushHandler cannot be extracted
     */
    private PushHandler unwrapPushHandler(RedisChannelWriter writer) {

        // If the writer itself is a PushHandler, return it directly
        if (writer instanceof PushHandler) {
            return (PushHandler) writer;
        }

        // Try to unwrap CommandExpiryWriter or MaintenanceAwareExpiryWriter
        // Both have a private 'delegate' field that contains the actual endpoint
        try {
            Field delegateField = writer.getClass().getDeclaredField("delegate");
            delegateField.setAccessible(true);
            Object delegate = delegateField.get(writer);

            if (delegate instanceof PushHandler) {
                return (PushHandler) delegate;
            }

            // If the delegate is still wrapped, try to unwrap it recursively
            if (delegate instanceof RedisChannelWriter) {
                return unwrapPushHandler((RedisChannelWriter) delegate);
            }

            throw new IllegalStateException("Delegate is not a PushHandler: " + delegate.getClass());

        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Cannot extract PushHandler from channel writer: " + writer.getClass(), e);
        }
    }

}
