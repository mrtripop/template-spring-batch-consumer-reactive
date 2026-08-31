package com.template.batchconsumer.adapter.out.kafka;

import com.template.batchconsumer.domain.model.ConsumerStatus;
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
        when(registry.getListenerContainer("sample-consumer")).thenReturn(container);
        adapter = new KafkaListenerLifecycleAdapter(registry);
    }

    @Test
    @DisplayName("start delegates to the underlying container")
    void startDelegatesToContainer() {
        adapter.start("sample-consumer");
        verify(container).start();
    }

    @Test
    @DisplayName("stop delegates to the underlying container")
    void stopDelegatesToContainer() {
        adapter.stop("sample-consumer");
        verify(container).stop();
    }

    @Test
    @DisplayName("pause delegates to the underlying container")
    void pauseDelegatesToContainer() {
        adapter.pause("sample-consumer");
        verify(container).pause();
    }

    @Test
    @DisplayName("resume delegates to the underlying container")
    void resumeDelegatesToContainer() {
        adapter.resume("sample-consumer");
        verify(container).resume();
    }

    @Test
    @DisplayName("status reports RUNNING for a running, unpaused container")
    void statusReflectsRunningContainer() {
        when(container.isRunning()).thenReturn(true);
        when(container.isContainerPaused()).thenReturn(false);

        var status = adapter.status("sample-consumer");

        assertThat(status.consumerId()).isEqualTo("sample-consumer");
        assertThat(status.state()).isEqualTo(ConsumerStatus.State.RUNNING);
    }

    @Test
    @DisplayName("status reports PAUSED for a running, paused container")
    void statusReflectsPausedContainer() {
        when(container.isRunning()).thenReturn(true);
        when(container.isContainerPaused()).thenReturn(true);

        assertThat(adapter.status("sample-consumer").state()).isEqualTo(ConsumerStatus.State.PAUSED);
    }

    @Test
    @DisplayName("status reports STOPPED for a non-running container")
    void statusReflectsStoppedContainer() {
        when(container.isRunning()).thenReturn(false);

        assertThat(adapter.status("sample-consumer").state()).isEqualTo(ConsumerStatus.State.STOPPED);
    }

    @Test
    @DisplayName("status throws for an unknown consumer id")
    void statusThrowsForUnknownConsumerId() {
        assertThatThrownBy(() -> adapter.status("missing-consumer"))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    @DisplayName("statuses lists every registered container")
    void statusesListsAllRegisteredContainers() {
        when(registry.getListenerContainerIds()).thenReturn(Set.of("sample-consumer"));
        when(container.isRunning()).thenReturn(true);
        when(container.isContainerPaused()).thenReturn(false);

        var statuses = adapter.statuses();

        assertThat(statuses).hasSize(1);
        assertThat(statuses.get(0).consumerId()).isEqualTo("sample-consumer");
    }
}
