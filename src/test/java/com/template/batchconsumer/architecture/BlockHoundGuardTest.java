package com.template.batchconsumer.architecture;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("BlockHound guard")
class BlockHoundGuardTest {

    @Test
    @DisplayName("a blocking call on a non-blocking scheduler is detected")
    void blockHoundDetectsBlockingCallOnNonBlockingScheduler() {
        // Arrange
        Mono<String> blockingMono = Mono.fromCallable(() -> {
                    Thread.sleep(10);
                    return "done";
                })
                .subscribeOn(Schedulers.parallel());

        // Act & Assert
        assertThatThrownBy(blockingMono::block)
                .hasCauseInstanceOf(BlockingOperationError.class);
    }
}
