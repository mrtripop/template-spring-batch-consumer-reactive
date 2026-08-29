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
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Standing proof (spec Decision Log #8) that a span created inside a reactive body, after a
 * thread hop, nests correctly under the span active at the {@code .block()} call site — the
 * same bridge pattern as {@code SampleKafkaListenerAdapter.onMessage} (Task 8): the reactive
 * chain is built and subscribed while the Kafka consumer span is current on the calling thread,
 * then blocked on.
 *
 * <p>This uses {@code io.opentelemetry.instrumentation.reactor.v3_1.ContextPropagationOperator}
 * — the same reactor-context-propagation library the production OTel javaagent registers
 * internally — so a pass here gives strong confidence the production agent-attached scenario
 * also propagates context correctly, without needing to attach the shaded javaagent inside a
 * Surefire-run JUnit test.
 *
 * <p><b>API-drift note:</b> the task plan assumed a {@code ContextPropagationOperator.INSTANCE}
 * singleton field (pattern used in older opentelemetry-instrumentation releases). As resolved in
 * this project ({@code opentelemetry-instrumentation-bom-alpha:2.28.1-alpha}, package path
 * confirmed unchanged via the resolved jar's contents), {@code ContextPropagationOperator} has no
 * {@code INSTANCE} field — instances are obtained via the static factory {@link
 * ContextPropagationOperator#create()} instead. The version and package path pinned in the plan
 * are otherwise correct; only this one API shape changed.
 */
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
    void spanCreatedInsideReactiveBodyNestsUnderKafkaConsumerSpan() {
        spanExporter.reset();
        MessageProcessor<String> processor = new SpanCreatingProcessor();
        MessageEnvelope<String> envelope =
                new MessageEnvelope<>("sample-consumer", "sample-events", 0, 0L, "key", "payload");

        Span consumerSpan = tracer.spanBuilder("sample-events process")
                .setSpanKind(SpanKind.CONSUMER)
                .startSpan();
        try (Scope scope = consumerSpan.makeCurrent()) {
            // Mirrors the production bridge: the Mono is built and subscribed while the
            // Kafka consumer span is current, then blocked on — same as
            // SampleKafkaListenerAdapter.onMessage's single blocking bridge point.
            processor.process(envelope).block(Duration.ofSeconds(5));
        } finally {
            consumerSpan.end();
        }

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
