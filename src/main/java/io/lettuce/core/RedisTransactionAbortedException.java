/*
 * Copyright 2025, Redis Ltd. and Contributors
 * All rights reserved.
 *
 * Licensed under the MIT License.
 */

package io.lettuce.core;

/**
 * Exception that gets thrown when Redis replies with a {@code ERR EXEC without MULTI} error response.
 *
 * @author Tihomir Mateev
 * @since 6.7
 */
@SuppressWarnings("serial")
public class RedisTransactionAbortedException extends RedisCommandExecutionException {

    /**
     * Create a {@code RedisTransactionAbortedException} with the specified detail message.
     *
     * @param msg the detail message.
     */
    public RedisTransactionAbortedException(String msg) {
        super(msg);
    }

    /**
     * Create a {@code RedisTransactionAbortedException} with the specified detail message and nested exception.
     *
     * @param msg the detail message.
     * @param cause the nested exception.
     */
    public RedisTransactionAbortedException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
