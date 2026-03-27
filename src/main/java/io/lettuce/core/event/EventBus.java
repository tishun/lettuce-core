package io.lettuce.core.event;

import java.io.Closeable;
import java.util.function.Consumer;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * Interface for an EventBus. Events can be published over the bus that are delivered to the subscribers.
 *
 * @author Mark Paluch
 * @since 3.4
 */
public interface EventBus {

    /**
     * Subscribe to events.
     *
     * @param onEvent callback invoked for each event
     * @return a {@link Closeable} that cancels the subscription when closed
     * @since 6.7
     */
    Closeable subscribe(Consumer<Event> onEvent);

    /**
     * Publish a {@link Event} to the bus.
     *
     * @param event the event to publish
     */
    void publish(Event event);

    /**
     * Subscribe to the event bus and {@link Event}s. The {@link Flux} drops events on backpressure to avoid contention.
     *
     * @return the observable to obtain events.
     * @deprecated since 6.7, use {@link #subscribe(Consumer)} instead, or use {@link #asFlux(EventBus)} for reactive access.
     */
    @Deprecated
    default Flux<Event> get() {
        return asFlux(this);
    }

    /**
     * Create a reactive {@link Flux} from an {@link EventBus}. Events are dropped on backpressure.
     *
     * @param bus the event bus to adapt
     * @return a {@link Flux} of events
     * @since 6.7
     */
    static Flux<Event> asFlux(EventBus bus) {
        return Flux.<Event> create(sink -> {
            Closeable subscription = bus.subscribe(sink::next);
            sink.onDispose(() -> {
                try {
                    subscription.close();
                } catch (Exception e) {
                    // won't happen for our implementation
                }
            });
        }, FluxSink.OverflowStrategy.DROP);
    }

}
