package com.template.batchconsumer.architecture;

import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlockHoundGuardTest {

    @Test
    void blockHoundDetectsBlockingCallOnNonBlockingScheduler() {
        Mono<String> blockingMono = Mono.fromCallable(() -> {
                    Thread.sleep(10);
                    return "done";
                })
                .subscribeOn(Schedulers.parallel());

        assertThatThrownBy(blockingMono::block)
                .hasCauseInstanceOf(BlockingOperationError.class);
    }
}
