package com.template.batchconsumer.observability;

import com.template.batchconsumer.application.port.in.MessageProcessor;
import com.template.batchconsumer.domain.model.MessageEnvelope;
import com.template.batchconsumer.domain.model.ProcessingOutcome;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.reactor.v3_1.ContextPropagationOperator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves a span created inside a reactive body, after a thread hop, nests correctly under the
 * span active at the {@code .block()} call site — the same bridge pattern as {@code
 * SampleKafkaListenerAdapter.onMessage}.
 */
@DisplayName("OpenTelemetry context propagation across a reactive thread hop")
class OtelContextPropagationTest {

    private static InMemorySpanExporter spanExporter;
    private static Tracer tracer;
    private static ContextPropagationOperator contextPropagationOperator;

    @BeforeAll
    static void setUpOpenTelemetry() {
        spanExporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();
        tracer = openTelemetry.getTracer("test");
        contextPropagationOperator = ContextPropagationOperator.create();
        contextPropagationOperator.registerOnEachOperator();
    }

    @AfterAll
    static void tearDownOpenTelemetry() {
        contextPropagationOperator.resetOnEachOperator();
    }

    /**
     * Stand-in for a real MessageProcessor that makes an instrumented downstream call —
     * creates a child span from inside the reactive body, on whatever thread Reactor
     * schedules that stage on (here, deliberately a different thread via boundedElastic).
     */
    private static final class SpanCreatingProcessor implements MessageProcessor<String> {
        @Override
        public Mono<ProcessingOutcome> process(MessageEnvelope<String> envelope) {
            return Mono.fromCallable(() -> {
                        Span childSpan = tracer.spanBuilder("downstream-call").startSpan();
                        childSpan.end();
                        return ProcessingOutcome.SUCCESS;
                    })
                    .subscribeOn(Schedulers.boundedElastic());
        }
    }

    @Test
    @DisplayName("a span created inside a reactive body nests under the Kafka consumer span")
    void spanCreatedInsideReactiveBodyNestsUnderKafkaConsumerSpan() {
        // Arrange
        spanExporter.reset();
        MessageProcessor<String> processor = new SpanCreatingProcessor();
        MessageEnvelope<String> envelope =
                new MessageEnvelope<>("sample-consumer", "sample-events", 0, 0L, "key", "payload");

        Span consumerSpan = tracer.spanBuilder("sample-events process")
                .setSpanKind(SpanKind.CONSUMER)
                .startSpan();

        // Act — mirrors SampleKafkaListenerAdapter.onMessage's bridge: built and subscribed
        // while the consumer span is current, then blocked on.
        try (Scope scope = consumerSpan.makeCurrent()) {
            processor.process(envelope).block(Duration.ofSeconds(5));
        } finally {
            consumerSpan.end();
        }

        // Assert
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        SpanData consumerSpanData = spans.stream()
                .filter(s -> s.getName().equals("sample-events process"))
                .findFirst()
                .orElseThrow();
        SpanData childSpanData = spans.stream()
                .filter(s -> s.getName().equals("downstream-call"))
                .findFirst()
                .orElseThrow();

        assertThat(childSpanData.getParentSpanId()).isEqualTo(consumerSpanData.getSpanId());
    }
}
