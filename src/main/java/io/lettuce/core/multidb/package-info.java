/*
 * Copyright 2011-Present, Redis Ltd. and Contributors
 * All rights reserved.
 *
 * Licensed under the MIT License.
 */

/**
 * Multi-database Redis client support.
 * 
 * <p>
 * This package provides support for managing multiple Redis database connections and switching between them dynamically. The
 * main components are:
 * </p>
 * 
 * <ul>
 * <li>{@link io.lettuce.core.multidb.MultiDatabaseRedisClient} - A Redis client that manages multiple database connections</li>
 * <li>{@link io.lettuce.core.multidb.SwitchableRedisConnection} - A connection that can switch between databases</li>
 * <li>{@link io.lettuce.core.multidb.SwitchableChannelWriter} - The underlying channel writer that handles database
 * switching</li>
 * </ul>
 * 
 * <h2>Usage Example</h2>
 * 
 * <pre>
 * 
 * {
 *     &#64;code
 *     // Create URIs for multiple databases
 *     Map<Integer, RedisURI> databaseURIs = new HashMap<>();
 *     databaseURIs.put(0, RedisURI.create("redis://localhost/0"));
 *     databaseURIs.put(1, RedisURI.create("redis://localhost/1"));
 *     databaseURIs.put(2, RedisURI.create("redis://localhost/2"));
 * 
 *     // Create the multi-database client with initial database 0
 *     MultiDatabaseRedisClient client = MultiDatabaseRedisClient.create(databaseURIs, 0);
 * 
 *     // Connect - all database connections are established upfront
 *     SwitchableRedisConnection<String, String> connection = client.connect();
 * 
 *     try {
 *         RedisCommands<String, String> sync = connection.sync();
 * 
 *         // Work with database 0
 *         sync.set("key", "value-in-db0");
 * 
 *         // Switch to database 1
 *         connection.switchDatabase(1);
 * 
 *         // Now working with database 1
 *         sync.set("key", "value-in-db1");
 * 
 *         // Switch back to database 0
 *         connection.switchDatabase(0);
 * 
 *         // See the original value
 *         String value = sync.get("key"); // Returns "value-in-db0"
 * 
 *     } finally {
 *         connection.close();
 *         client.shutdown();
 *     }
 * }
 * </pre>
 * 
 * <h2>Thread Safety</h2>
 * 
 * <p>
 * All components in this package are thread-safe. Database switching is protected by read-write locks to ensure that:
 * </p>
 * 
 * <ul>
 * <li>Multiple threads can write commands concurrently to the active database</li>
 * <li>Database switches are atomic and exclusive</li>
 * <li>In-flight commands are cancelled when switching databases</li>
 * </ul>
 * 
 * <h2>Command Cancellation</h2>
 * 
 * <p>
 * When switching databases, all in-flight commands on the previous database are cancelled with a
 * {@link java.util.concurrent.CancellationException}. This ensures that no commands are executed on the wrong database after a
 * switch.
 * </p>
 * 
 * <h2>Connection Lifecycle</h2>
 * 
 * <p>
 * All database connections are established upfront when calling
 * {@link io.lettuce.core.multidb.MultiDatabaseRedisClient#connect()}. This ensures that any connection failures are detected
 * early. The connections remain open until the {@link io.lettuce.core.multidb.SwitchableRedisConnection} is closed.
 * </p>
 * 
 * <h2>Error Handling</h2>
 * 
 * <p>
 * Failures in non-active connections are handled with best effort:
 * </p>
 * 
 * <ul>
 * <li>Connection failures during initial setup will fail the entire connect operation</li>
 * <li>Failures in non-active connections after setup are logged but do not affect the active connection</li>
 * <li>Switching to a database with a failed connection will fail with an appropriate exception</li>
 * </ul>
 * 
 * @since 6.6
 */
package io.lettuce.core.multidb;
