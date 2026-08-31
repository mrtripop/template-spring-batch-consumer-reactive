package com.template.batchconsumer.adapter.out.kafka;

import com.template.batchconsumer.domain.model.ConsumerStatus;
import com.template.batchconsumer.fixture.SampleFixture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.NoSuchElementException;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("Kafka listener lifecycle adapter")
class KafkaListenerLifecycleAdapterTest {

    private KafkaListenerEndpointRegistry registry;
    private MessageListenerContainer container;
    private KafkaListenerLifecycleAdapter adapter;

    @BeforeEach
    void setUp() {
        registry = Mockito.mock(KafkaListenerEndpointRegistry.class);
        container = Mockito.mock(MessageListenerContainer.class);
        when(registry.getListenerContainer(SampleFixture.CONSUMER_ID)).thenReturn(container);
        adapter = new KafkaListenerLifecycleAdapter(registry);
    }

    @Test
    @DisplayName("start delegates to the underlying container")
    void startDelegatesToContainer() {
        adapter.start(SampleFixture.CONSUMER_ID);
        verify(container).start();
    }

    @Test
    @DisplayName("stop delegates to the underlying container")
    void stopDelegatesToContainer() {
        adapter.stop(SampleFixture.CONSUMER_ID);
        verify(container).stop();
    }

    @Test
    @DisplayName("pause delegates to the underlying container")
    void pauseDelegatesToContainer() {
        adapter.pause(SampleFixture.CONSUMER_ID);
        verify(container).pause();
    }

    @Test
    @DisplayName("resume delegates to the underlying container")
    void resumeDelegatesToContainer() {
        adapter.resume(SampleFixture.CONSUMER_ID);
        verify(container).resume();
    }

    @Test
    @DisplayName("status reports RUNNING for a running, unpaused container")
    void statusReflectsRunningContainer() {
        when(container.isRunning()).thenReturn(true);
        when(container.isContainerPaused()).thenReturn(false);

        var status = adapter.status(SampleFixture.CONSUMER_ID);

        assertThat(status.consumerId()).isEqualTo(SampleFixture.CONSUMER_ID);
        assertThat(status.state()).isEqualTo(ConsumerStatus.State.RUNNING);
    }

    @Test
    @DisplayName("status reports PAUSED for a running, paused container")
    void statusReflectsPausedContainer() {
        when(container.isRunning()).thenReturn(true);
        when(container.isContainerPaused()).thenReturn(true);

        assertThat(adapter.status(SampleFixture.CONSUMER_ID).state()).isEqualTo(ConsumerStatus.State.PAUSED);
    }

    @Test
    @DisplayName("status reports STOPPED for a non-running container")
    void statusReflectsStoppedContainer() {
        when(container.isRunning()).thenReturn(false);

        assertThat(adapter.status(SampleFixture.CONSUMER_ID).state()).isEqualTo(ConsumerStatus.State.STOPPED);
    }

    @Test
    @DisplayName("status throws for an unknown consumer id")
    void statusThrowsForUnknownConsumerId() {
        assertThatThrownBy(() -> adapter.status(SampleFixture.UNKNOWN_CONSUMER_ID))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    @DisplayName("statuses lists every registered container")
    void statusesListsAllRegisteredContainers() {
        when(registry.getListenerContainerIds()).thenReturn(Set.of(SampleFixture.CONSUMER_ID));
        when(container.isRunning()).thenReturn(true);
        when(container.isContainerPaused()).thenReturn(false);

        var statuses = adapter.statuses();

        assertThat(statuses).hasSize(1);
        assertThat(statuses.get(0).consumerId()).isEqualTo(SampleFixture.CONSUMER_ID);
    }
}
