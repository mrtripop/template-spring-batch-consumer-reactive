package com.template.batchconsumer.adapter.out.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    void startDelegatesToContainer() {
        adapter.start("sample-consumer");
        verify(container).start();
    }

    @Test
    void stopDelegatesToContainer() {
        adapter.stop("sample-consumer");
        verify(container).stop();
    }

    @Test
    void pauseDelegatesToContainer() {
        adapter.pause("sample-consumer");
        verify(container).pause();
    }

    @Test
    void resumeDelegatesToContainer() {
        adapter.resume("sample-consumer");
        verify(container).resume();
    }

    @Test
    void statusReflectsRunningContainer() {
        when(container.isRunning()).thenReturn(true);
        when(container.isContainerPaused()).thenReturn(false);

        var status = adapter.status("sample-consumer");

        org.assertj.core.api.Assertions.assertThat(status.consumerId()).isEqualTo("sample-consumer");
        org.assertj.core.api.Assertions.assertThat(status.state())
                .isEqualTo(com.template.batchconsumer.domain.model.ConsumerStatus.State.RUNNING);
    }

    @Test
    void statusReflectsPausedContainer() {
        when(container.isRunning()).thenReturn(true);
        when(container.isContainerPaused()).thenReturn(true);

        org.assertj.core.api.Assertions.assertThat(adapter.status("sample-consumer").state())
                .isEqualTo(com.template.batchconsumer.domain.model.ConsumerStatus.State.PAUSED);
    }

    @Test
    void statusReflectsStoppedContainer() {
        when(container.isRunning()).thenReturn(false);

        org.assertj.core.api.Assertions.assertThat(adapter.status("sample-consumer").state())
                .isEqualTo(com.template.batchconsumer.domain.model.ConsumerStatus.State.STOPPED);
    }

    @Test
    void statusThrowsForUnknownConsumerId() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> adapter.status("missing-consumer"))
                .isInstanceOf(java.util.NoSuchElementException.class);
    }

    @Test
    void statusesListsAllRegisteredContainers() {
        when(registry.getListenerContainerIds()).thenReturn(java.util.Set.of("sample-consumer"));
        when(container.isRunning()).thenReturn(true);
        when(container.isContainerPaused()).thenReturn(false);

        var statuses = adapter.statuses();

        org.assertj.core.api.Assertions.assertThat(statuses).hasSize(1);
        org.assertj.core.api.Assertions.assertThat(statuses.get(0).consumerId()).isEqualTo("sample-consumer");
    }
}
