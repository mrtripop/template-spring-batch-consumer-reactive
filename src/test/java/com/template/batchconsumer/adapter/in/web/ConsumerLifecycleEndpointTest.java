package com.template.batchconsumer.adapter.in.web;

import com.template.batchconsumer.application.port.in.ConsumerLifecycleUseCase;
import com.template.batchconsumer.domain.model.ConsumerStatus;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;

import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;

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

        WebEndpointResponse<ConsumerStatus> response = endpoint.status("sample-consumer");

        assertThat(response.getStatus()).isEqualTo(WebEndpointResponse.STATUS_OK);
        assertThat(response.getBody()).isEqualTo(status);
    }

    @Test
    void statusReturnsNotFoundForUnknownConsumerId() {
        when(useCase.status("does-not-exist")).thenThrow(new NoSuchElementException("no such container"));

        WebEndpointResponse<ConsumerStatus> response = endpoint.status("does-not-exist");

        assertThat(response.getStatus()).isEqualTo(WebEndpointResponse.STATUS_NOT_FOUND);
    }

    @Test
    void applyActionPauseCallsPause() {
        WebEndpointResponse<Void> response = endpoint.applyAction("sample-consumer", ConsumerLifecycleEndpoint.Action.PAUSE);
        org.mockito.Mockito.verify(useCase).pause("sample-consumer");
        assertThat(response.getStatus()).isEqualTo(WebEndpointResponse.STATUS_OK);
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

    @Test
    void applyActionReturnsNotFoundForUnknownConsumerId() {
        org.mockito.Mockito.doThrow(new NoSuchElementException("no such container"))
                .when(useCase).pause("does-not-exist");

        WebEndpointResponse<Void> response =
                endpoint.applyAction("does-not-exist", ConsumerLifecycleEndpoint.Action.PAUSE);

        assertThat(response.getStatus()).isEqualTo(WebEndpointResponse.STATUS_NOT_FOUND);
    }
}
