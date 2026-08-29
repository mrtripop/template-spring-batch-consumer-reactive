package com.template.batchconsumer.adapter.in.web;

import com.template.batchconsumer.application.port.in.ConsumerLifecycleUseCase;
import com.template.batchconsumer.domain.model.ConsumerStatus;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

class ConsumerLifecycleEndpointTest {

    private final ConsumerLifecycleUseCase useCase = Mockito.mock(ConsumerLifecycleUseCase.class);
    private final ConsumerLifecycleEndpoint endpoint = new ConsumerLifecycleEndpoint(useCase);

    @Test
    void statusesDelegatesToUseCase() {
        ConsumerStatus status = new ConsumerStatus("sample-consumer", ConsumerStatus.State.RUNNING, Instant.now());
        when(useCase.statuses()).thenReturn(List.of(status));

        assertThat(endpoint.statuses()).containsExactly(status);
    }

    @Test
    void statusDelegatesToUseCaseForGivenId() {
        ConsumerStatus status = new ConsumerStatus("sample-consumer", ConsumerStatus.State.PAUSED, Instant.now());
        when(useCase.status("sample-consumer")).thenReturn(status);

        assertThat(endpoint.status("sample-consumer")).isEqualTo(status);
    }

    @Test
    void applyActionPauseCallsPause() {
        endpoint.applyAction("sample-consumer", ConsumerLifecycleEndpoint.Action.PAUSE);
        org.mockito.Mockito.verify(useCase).pause("sample-consumer");
    }

    @Test
    void applyActionResumeCallsResume() {
        endpoint.applyAction("sample-consumer", ConsumerLifecycleEndpoint.Action.RESUME);
        org.mockito.Mockito.verify(useCase).resume("sample-consumer");
    }

    @Test
    void applyActionStopCallsStop() {
        endpoint.applyAction("sample-consumer", ConsumerLifecycleEndpoint.Action.STOP);
        org.mockito.Mockito.verify(useCase).stop("sample-consumer");
    }

    @Test
    void applyActionStartCallsStart() {
        endpoint.applyAction("sample-consumer", ConsumerLifecycleEndpoint.Action.START);
        org.mockito.Mockito.verify(useCase).start("sample-consumer");
    }
}
