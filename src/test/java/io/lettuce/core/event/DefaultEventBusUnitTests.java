package io.lettuce.core.event;

import static io.lettuce.TestTags.UNIT_TEST;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Closeable;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import reactor.core.Disposable;
import reactor.test.StepVerifier;

/**
 * @author Mark Paluch
 */
@Tag(UNIT_TEST)
@ExtendWith(MockitoExtension.class)
class DefaultEventBusUnitTests {

    @Mock
    private Event event;

    @Test
    void publishToSubscriber() {
        EventBus sut = new DefaultEventBus();
        ArrayBlockingQueue<Event> queue = new ArrayBlockingQueue<>(5);

        sut.subscribe(queue::add);
        sut.publish(event);

        assertThat(queue).containsExactly(event);
    }

    @Test
    void publishToSubscriberViaFlux() {
        EventBus sut = new DefaultEventBus();

        StepVerifier.create(sut.get()).then(() -> sut.publish(event)).expectNext(event).thenCancel().verify();
    }

    @Test
    void publishToMultipleSubscribers() throws Exception {
        EventBus sut = new DefaultEventBus();
        ArrayBlockingQueue<Event> queue1 = new ArrayBlockingQueue<>(5);
        ArrayBlockingQueue<Event> queue2 = new ArrayBlockingQueue<>(5);

        Closeable sub1 = sut.subscribe(queue1::add);
        sut.subscribe(queue2::add);

        sut.publish(event);

        assertThat(queue1.take()).isEqualTo(event);
        assertThat(queue2.take()).isEqualTo(event);

        sub1.close();
    }

    @Test
    void publishToMultipleSubscribersViaFlux() throws Exception {
        EventBus sut = new DefaultEventBus();
        ArrayBlockingQueue<Event> arrayQueue = new ArrayBlockingQueue<>(5);

        Disposable disposable1 = sut.get().doOnNext(arrayQueue::add).subscribe();
        StepVerifier.create(sut.get().doOnNext(arrayQueue::add)).then(() -> sut.publish(event)).expectNext(event).thenCancel()
                .verify();

        assertThat(arrayQueue.take()).isEqualTo(event);
        assertThat(arrayQueue.take()).isEqualTo(event);
        disposable1.dispose();
    }

    @Test
    void unsubscribeRemovesSubscriber() throws Exception {
        EventBus sut = new DefaultEventBus();
        ArrayBlockingQueue<Event> queue = new ArrayBlockingQueue<>(5);

        Closeable subscription = sut.subscribe(queue::add);
        sut.publish(event);
        assertThat(queue).hasSize(1);

        subscription.close();
        sut.publish(event);
        assertThat(queue).hasSize(1); // still 1, no new event received
    }

}
