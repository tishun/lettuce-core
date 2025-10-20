/*
 * Copyright 2011-Present, Redis Ltd. and Contributors
 * All rights reserved.
 *
 * Licensed under the MIT License.
 */
package io.lettuce.core.multidb;

import static org.assertj.core.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisURI;
import io.lettuce.core.TestSupport;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.test.settings.TestSettings;

/**
 * Integration tests for {@link MultiDatabaseRedisClient}.
 *
 * @author Tihomir Mateev
 */
class MultiDatabaseRedisClientIntegrationTests extends TestSupport {

    private MultiDatabaseRedisClient client;

    private SwitchableRedisConnection<String, String> connection;

    @BeforeEach
    void setUp() {
        // Create URIs for databases 0, 1, and 2
        Map<Integer, RedisURI> databaseURIs = new HashMap<>();

        RedisURI baseUri = RedisURI.Builder.redis(TestSettings.host(), TestSettings.port()).build();

        for (int i = 0; i <= 2; i++) {
            RedisURI uri = RedisURI.Builder.redis(TestSettings.host(), TestSettings.port()).withDatabase(i).build();
            databaseURIs.put(i, uri);
        }

        client = MultiDatabaseRedisClient.create(databaseURIs, 0);
        connection = client.connect();
    }

    @AfterEach
    void tearDown() {
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
        }
    }

    @Test
    void shouldConnectToInitialDatabase() {
        assertThat(connection.getActiveDatabase()).isEqualTo(0);
        assertThat(connection.isOpen()).isTrue();
    }

    @Test
    void shouldWriteToDatabase0() {
        RedisCommands<String, String> sync = connection.sync();

        sync.set("key0", "value0");
        String value = sync.get("key0");

        assertThat(value).isEqualTo("value0");

        sync.del("key0");
    }

    @Test
    void shouldSwitchDatabases() {
        RedisCommands<String, String> sync = connection.sync();

        // Write to database 0
        sync.set("key", "db0");
        assertThat(sync.get("key")).isEqualTo("db0");

        // Switch to database 1
        connection.switchDatabase(1);
        assertThat(connection.getActiveDatabase()).isEqualTo(1);

        // Key should not exist in database 1
        assertThat(sync.get("key")).isNull();

        // Write to database 1
        sync.set("key", "db1");
        assertThat(sync.get("key")).isEqualTo("db1");

        // Switch back to database 0
        connection.switchDatabase(0);
        assertThat(connection.getActiveDatabase()).isEqualTo(0);

        // Should see the original value
        assertThat(sync.get("key")).isEqualTo("db0");

        // Cleanup
        sync.del("key");
        connection.switchDatabase(1);
        sync.del("key");
        connection.switchDatabase(0);
    }

    @Test
    void shouldIsolateDatabases() {
        RedisCommands<String, String> sync = connection.sync();

        // Write different values to each database
        for (int db = 0; db <= 2; db++) {
            connection.switchDatabase(db);
            sync.set("isolation-test", "db" + db);
        }

        // Verify each database has its own value
        for (int db = 0; db <= 2; db++) {
            connection.switchDatabase(db);
            assertThat(sync.get("isolation-test")).isEqualTo("db" + db);
        }

        // Cleanup
        for (int db = 0; db <= 2; db++) {
            connection.switchDatabase(db);
            sync.del("isolation-test");
        }
        connection.switchDatabase(0);
    }

    @Test
    void shouldHandleConcurrentSwitches() throws InterruptedException {
        RedisCommands<String, String> sync = connection.sync();
        int threadCount = 10;
        int operationsPerThread = 100;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < operationsPerThread; i++) {
                        int targetDb = i % 3;

                        // Switch database
                        connection.switchDatabase(targetDb);

                        // Verify we're on the right database
                        int currentDb = connection.getActiveDatabase();
                        if (currentDb == targetDb) {
                            successCount.incrementAndGet();
                        }

                        // Small delay to increase contention
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    // Expected due to command cancellation during switches
                } finally {
                    latch.countDown();
                }
            });
        }

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        // We should have had many successful operations
        assertThat(successCount.get()).isGreaterThan(0);
    }

    @Test
    void shouldRejectInvalidDatabase() {
        assertThatThrownBy(() -> connection.switchDatabase(99)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No writer available for database");
    }

    @Test
    void shouldHandleMultipleConnections() {
        SwitchableRedisConnection<String, String> connection2 = client.connect();

        try {
            RedisCommands<String, String> sync1 = connection.sync();
            RedisCommands<String, String> sync2 = connection2.sync();

            // Both connections start at database 0
            assertThat(connection.getActiveDatabase()).isEqualTo(0);
            assertThat(connection2.getActiveDatabase()).isEqualTo(0);

            // Switch first connection to database 1
            connection.switchDatabase(1);
            sync1.set("multi-conn", "conn1");

            // Second connection should still be on database 0
            assertThat(connection2.getActiveDatabase()).isEqualTo(0);
            assertThat(sync2.get("multi-conn")).isNull();

            // Switch second connection to database 1
            connection2.switchDatabase(1);
            assertThat(sync2.get("multi-conn")).isEqualTo("conn1");

            // Cleanup
            sync2.del("multi-conn");

        } finally {
            connection2.close();
        }
    }

    @Test
    void shouldCloseGracefully() {
        RedisCommands<String, String> sync = connection.sync();

        sync.set("close-test", "value");
        assertThat(sync.get("close-test")).isEqualTo("value");

        sync.del("close-test");

        connection.close();

        assertThat(connection.isOpen()).isFalse();
    }

}
