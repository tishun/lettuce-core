/*
 * Copyright 2011-Present, Redis Ltd. and Contributors
 * All rights reserved.
 *
 * Licensed under the MIT License.
 */
package io.lettuce.core.multidb;

import java.time.Duration;
import java.util.Map;

import io.lettuce.core.RedisChannelWriter;
import io.lettuce.core.StatefulRedisConnectionImpl;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.PushHandler;

/**
 * A {@link io.lettuce.core.api.StatefulRedisConnection} that can switch between multiple databases. This connection uses a
 * {@link SwitchableChannelWriter} to delegate commands to different underlying connections based on the currently active
 * database.
 * 
 * @param <K> Key type.
 * @param <V> Value type.
 * @author Tihomir Mateev
 * @since 6.6
 */
public class SwitchableRedisConnection<K, V> extends StatefulRedisConnectionImpl<K, V> {

    private final SwitchableChannelWriter switchableWriter;

    /**
     * Initialize a new switchable connection.
     *
     * @param writers map of database index to channel writers, must not be {@code null}
     * @param initialDatabase the initial active database index
     * @param pushHandler the handler for push notifications, must not be {@code null}
     * @param codec Codec used to encode/decode keys and values, must not be {@code null}
     * @param timeout Maximum time to wait for a response, must not be {@code null}
     */
    public SwitchableRedisConnection(Map<Integer, RedisChannelWriter> writers, int initialDatabase, PushHandler pushHandler,
            RedisCodec<K, V> codec, Duration timeout) {

        this(new SwitchableChannelWriter(writers, initialDatabase, writers.get(initialDatabase).getClientResources()),
                pushHandler, codec, timeout);
    }

    /**
     * Initialize a new switchable connection with a pre-configured switchable writer.
     *
     * @param switchableWriter the switchable channel writer, must not be {@code null}
     * @param pushHandler the handler for push notifications, must not be {@code null}
     * @param codec Codec used to encode/decode keys and values, must not be {@code null}
     * @param timeout Maximum time to wait for a response, must not be {@code null}
     */
    public SwitchableRedisConnection(SwitchableChannelWriter switchableWriter, PushHandler pushHandler, RedisCodec<K, V> codec,
            Duration timeout) {

        super(switchableWriter, pushHandler, codec, timeout);

        LettuceAssert.notNull(switchableWriter, "SwitchableChannelWriter must not be null");
        this.switchableWriter = switchableWriter;
    }

    /**
     * Switch to a different database. This will cancel all in-flight commands on the current database.
     *
     * @param databaseIndex the database index to switch to
     * @throws IllegalArgumentException if the database index is not available
     * @throws IllegalStateException if the connection is closed
     */
    public void switchDatabase(int databaseIndex) {
        switchableWriter.switchDatabase(databaseIndex);
    }

    /**
     * Get the currently active database index.
     *
     * @return the active database index
     */
    public int getActiveDatabase() {
        return switchableWriter.getActiveDatabase();
    }

}
