package io.lettuce.core.event;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

import io.lettuce.core.event.jfr.EventRecorder;

/**
 * Default implementation for an {@link EventBus}. Events are recorded through {@link EventRecorder#record(Event) EventRecorder}
 * and delivered to all subscribed consumers.
 *
 * @author Mark Paluch
 * @since 3.4
 */
public class DefaultEventBus implements EventBus {

    private final Set<Consumer<Event>> subscribers = new CopyOnWriteArraySet<>();

    private final EventRecorder recorder = EventRecorder.getInstance();

    public DefaultEventBus() {
    }

    @Override
    public Closeable subscribe(Consumer<Event> onEvent) {
        subscribers.add(onEvent);
        return () -> subscribers.remove(onEvent);
    }

    @Override
    public void publish(Event event) {
        recorder.record(event);
        for (Consumer<Event> subscriber : subscribers) {
            subscriber.accept(event);
        }
    }

}
