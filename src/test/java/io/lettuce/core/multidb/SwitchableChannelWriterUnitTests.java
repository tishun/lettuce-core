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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.lettuce.core.RedisChannelWriter;
import io.lettuce.core.RedisException;
import io.lettuce.core.protocol.ConnectionFacade;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.resource.ClientResources;

/**
 * Unit tests for {@link SwitchableChannelWriter}.
 *
 * @author Tihomir Mateev
 */
@ExtendWith(MockitoExtension.class)
class SwitchableChannelWriterUnitTests {

    @Mock
    private RedisChannelWriter writer0;

    @Mock
    private RedisChannelWriter writer1;

    @Mock
    private RedisChannelWriter writer2;

    @Mock
    private ClientResources clientResources;

    @Mock
    private RedisCommand<String, String, String> command;

    @Mock
    private ConnectionFacade connectionFacade;

    private Map<Integer, RedisChannelWriter> writers;

    private SwitchableChannelWriter switchableWriter;

    @BeforeEach
    void setUp() {
        writers = new HashMap<>();
        writers.put(0, writer0);
        writers.put(1, writer1);
        writers.put(2, writer2);

        lenient().when(writer0.getClientResources()).thenReturn(clientResources);
        lenient().when(writer1.getClientResources()).thenReturn(clientResources);
        lenient().when(writer2.getClientResources()).thenReturn(clientResources);

        switchableWriter = new SwitchableChannelWriter(writers, 0, clientResources);
    }

    @Test
    void shouldCreateWithInitialDatabase() {
        assertThat(switchableWriter.getActiveDatabase()).isEqualTo(0);
    }

    @Test
    void shouldRejectNullWriters() {
        assertThatThrownBy(() -> new SwitchableChannelWriter(null, 0, clientResources))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Writers must not be null");
    }

    @Test
    void shouldRejectEmptyWriters() {
        assertThatThrownBy(() -> new SwitchableChannelWriter(new HashMap<>(), 0, clientResources))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Writers must not be empty");
    }

    @Test
    void shouldRejectInvalidInitialDatabase() {
        assertThatThrownBy(() -> new SwitchableChannelWriter(writers, 99, clientResources))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Writers must contain the initial database");
    }

    @Test
    void shouldWriteToActiveWriter() {
        when(writer0.write(command)).thenReturn(command);

        RedisCommand<String, String, String> result = switchableWriter.write(command);

        assertThat(result).isSameAs(command);
        verify(writer0).write(command);
        verifyNoInteractions(writer1, writer2);
    }

    @Test
    void shouldSwitchDatabase() {
        switchableWriter.switchDatabase(1);

        assertThat(switchableWriter.getActiveDatabase()).isEqualTo(1);

        when(writer1.write(command)).thenReturn(command);
        switchableWriter.write(command);

        verify(writer1).write(command);
        verifyNoInteractions(writer0, writer2);
    }

    @Test
    void shouldNotSwitchToSameDatabase() {
        switchableWriter.switchDatabase(0);

        assertThat(switchableWriter.getActiveDatabase()).isEqualTo(0);
        verifyNoInteractions(writer0, writer1, writer2);
    }

    @Test
    void shouldRejectSwitchToInvalidDatabase() {
        assertThatThrownBy(() -> switchableWriter.switchDatabase(99)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No writer available for database");
    }

    // Note: We cannot test command cancellation directly because HasQueuedCommands is package-private.
    // The cancellation logic is tested indirectly through integration tests.

    @Test
    void shouldSetConnectionFacadeOnAllWriters() {
        switchableWriter.setConnectionFacade(connectionFacade);

        verify(writer0).setConnectionFacade(connectionFacade);
        verify(writer1).setConnectionFacade(connectionFacade);
        verify(writer2).setConnectionFacade(connectionFacade);
    }

    @Test
    void shouldSetAutoFlushOnActiveWriter() {
        switchableWriter.setAutoFlushCommands(true);

        verify(writer0).setAutoFlushCommands(true);
        verifyNoInteractions(writer1, writer2);
    }

    @Test
    void shouldFlushActiveWriter() {
        switchableWriter.flushCommands();

        verify(writer0).flushCommands();
        verifyNoInteractions(writer1, writer2);
    }

    @Test
    void shouldCloseAllWriters() {
        switchableWriter.close();

        verify(writer0).close();
        verify(writer1).close();
        verify(writer2).close();
    }

    @Test
    void shouldCloseAllWritersAsync() {
        when(writer0.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(writer1.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(writer2.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> future = switchableWriter.closeAsync();

        assertThat(future).isCompleted();
        verify(writer0).closeAsync();
        verify(writer1).closeAsync();
        verify(writer2).closeAsync();
    }

    @Test
    void shouldRejectWriteAfterClose() {
        switchableWriter.close();

        switchableWriter.write(command);

        verify(command).completeExceptionally(any(RedisException.class));
    }

    @Test
    void shouldRejectSwitchAfterClose() {
        switchableWriter.close();

        assertThatThrownBy(() -> switchableWriter.switchDatabase(1)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SwitchableChannelWriter is closed");
    }

    @Test
    void shouldReturnClientResources() {
        assertThat(switchableWriter.getClientResources()).isSameAs(clientResources);
    }

}
