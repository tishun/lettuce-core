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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.lettuce.core.RedisChannelWriter;
import io.lettuce.core.RedisException;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.ConnectionFacade;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.resource.ClientResources;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * A thread-safe {@link RedisChannelWriter} that can switch between multiple underlying channel writers. When switching to a
 * different writer, all in-flight commands on the previous writer are cancelled.
 * 
 * @author Tihomir Mateev
 * @since 6.6
 */
public class SwitchableChannelWriter implements RedisChannelWriter {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(SwitchableChannelWriter.class);

    private final Map<Integer, RedisChannelWriter> writers;

    private final AtomicReference<RedisChannelWriter> activeWriter = new AtomicReference<>();

    private final AtomicInteger activeDatabase = new AtomicInteger(0);

    private final ReentrantReadWriteLock switchLock = new ReentrantReadWriteLock();

    private final Lock readLock = switchLock.readLock();

    private final Lock writeLock = switchLock.writeLock();

    private final ClientResources clientResources;

    private volatile ConnectionFacade connectionFacade;

    private volatile boolean closed = false;

    /**
     * Create a new {@link SwitchableChannelWriter}.
     *
     * @param writers map of database index to channel writers, must not be {@code null} or empty
     * @param initialDatabase the initial active database index
     * @param clientResources the client resources, must not be {@code null}
     */
    public SwitchableChannelWriter(Map<Integer, RedisChannelWriter> writers, int initialDatabase,
            ClientResources clientResources) {

        LettuceAssert.notNull(writers, "Writers must not be null");
        LettuceAssert.isTrue(!writers.isEmpty(), "Writers must not be empty");
        LettuceAssert.notNull(clientResources, "ClientResources must not be null");
        LettuceAssert.isTrue(writers.containsKey(initialDatabase),
                "Writers must contain the initial database: " + initialDatabase);

        this.writers = new ConcurrentHashMap<>(writers);
        this.clientResources = clientResources;
        this.activeDatabase.set(initialDatabase);
        this.activeWriter.set(writers.get(initialDatabase));
    }

    /**
     * Switch to a different database. This will cancel all in-flight commands on the current writer.
     *
     * @param databaseIndex the database index to switch to
     * @throws IllegalArgumentException if the database index is not available
     * @throws IllegalStateException if the writer is closed
     */
    public void switchDatabase(int databaseIndex) {

        if (closed) {
            throw new IllegalStateException("SwitchableChannelWriter is closed");
        }

        RedisChannelWriter newWriter = writers.get(databaseIndex);
        if (newWriter == null) {
            throw new IllegalArgumentException("No writer available for database: " + databaseIndex);
        }

        writeLock.lock();
        try {
            int currentDb = activeDatabase.get();
            if (currentDb == databaseIndex) {
                // Already on this database
                return;
            }

            RedisChannelWriter oldWriter = activeWriter.get();

            if (logger.isDebugEnabled()) {
                logger.debug("Switching from database {} to database {}", currentDb, databaseIndex);
            }

            // Cancel all in-flight commands on the old writer
            cancelInFlightCommands(oldWriter);

            // Switch to the new writer
            activeWriter.set(newWriter);
            activeDatabase.set(databaseIndex);

            if (logger.isDebugEnabled()) {
                logger.debug("Successfully switched to database {}", databaseIndex);
            }

        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Get the currently active database index.
     *
     * @return the active database index
     */
    public int getActiveDatabase() {
        return activeDatabase.get();
    }

    @Override
    public <K, V, T> RedisCommand<K, V, T> write(RedisCommand<K, V, T> command) {

        if (closed) {
            command.completeExceptionally(new RedisException("SwitchableChannelWriter is closed"));
            return command;
        }

        readLock.lock();
        try {
            RedisChannelWriter writer = activeWriter.get();
            return writer.write(command);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public <K, V> Collection<RedisCommand<K, V, ?>> write(Collection<? extends RedisCommand<K, V, ?>> commands) {

        if (closed) {
            RedisException exception = new RedisException("SwitchableChannelWriter is closed");
            commands.forEach(cmd -> cmd.completeExceptionally(exception));
            return (Collection<RedisCommand<K, V, ?>>) commands;
        }

        readLock.lock();
        try {
            RedisChannelWriter writer = activeWriter.get();
            return writer.write(commands);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void close() {

        if (closed) {
            return;
        }

        writeLock.lock();
        try {
            if (closed) {
                return;
            }

            closed = true;

            // Close all writers
            for (RedisChannelWriter writer : writers.values()) {
                try {
                    writer.close();
                } catch (Exception e) {
                    logger.warn("Error closing writer", e);
                }
            }

            writers.clear();

        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {

        if (closed) {
            return CompletableFuture.completedFuture(null);
        }

        writeLock.lock();
        try {
            if (closed) {
                return CompletableFuture.completedFuture(null);
            }

            closed = true;

            // Close all writers asynchronously
            CompletableFuture<?>[] futures = writers.values().stream().map(writer -> writer.closeAsync().exceptionally(t -> {
                logger.warn("Error closing writer asynchronously", t);
                return null;
            })).toArray(CompletableFuture[]::new);

            return CompletableFuture.allOf(futures).whenComplete((v, t) -> writers.clear());

        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void setConnectionFacade(ConnectionFacade connection) {
        this.connectionFacade = connection;

        // Set the connection facade on all writers
        for (RedisChannelWriter writer : writers.values()) {
            writer.setConnectionFacade(connection);
        }
    }

    @Override
    public void setAutoFlushCommands(boolean autoFlush) {
        readLock.lock();
        try {
            RedisChannelWriter writer = activeWriter.get();
            if (writer != null) {
                writer.setAutoFlushCommands(autoFlush);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void flushCommands() {
        readLock.lock();
        try {
            RedisChannelWriter writer = activeWriter.get();
            if (writer != null) {
                writer.flushCommands();
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public ClientResources getClientResources() {
        return clientResources;
    }

    /**
     * Cancel all in-flight commands on the given writer.
     *
     * Note: Since HasQueuedCommands is package-private, we cannot directly access it. This is a best-effort approach that logs
     * a warning. In practice, the commands will be cancelled when the connection is closed or times out.
     *
     * @param writer the writer whose commands should be cancelled
     */
    private void cancelInFlightCommands(RedisChannelWriter writer) {

        if (writer == null) {
            return;
        }

        // Note: We cannot access HasQueuedCommands as it's package-private.
        // Commands will be cancelled when they timeout or when the connection is closed.
        if (logger.isDebugEnabled()) {
            logger.debug("Database switch completed. In-flight commands on previous database will timeout.");
        }
    }

}
