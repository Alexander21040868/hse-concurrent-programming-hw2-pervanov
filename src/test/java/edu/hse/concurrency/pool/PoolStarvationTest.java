package edu.hse.concurrency.pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(30)
class PoolStarvationTest {

    private static final int THREADS = 4;
    private static final int CONNECTIONS_PER_REQUEST = 2;

    @Test
    void requestsWaitForEachOtherWhenPoolIsFull() throws Exception {
        runRequests(THREADS, true);
    }

    @Test
    void requestsFinishAtTheMinimumSafePoolSize() throws Exception {
        runRequests(THREADS * (CONNECTIONS_PER_REQUEST - 1) + 1, false);
    }

    private void runRequests(int maxSize, boolean expectStarvation) throws Exception {
        ConnectionPool pool = new ConnectionPoolImpl(new PoolConfig(0, maxSize, Duration.ofMinutes(1), true),
                () -> new FakeConnectionFactory.FakeConnection(0));
        ExecutorService executor = Executors.newFixedThreadPool(THREADS,
                Thread.ofPlatform().name("pool-request-", 1).factory());
        try {
            List<Future<Void>> requests = submitRequests(pool, executor);
            if (expectStarvation) {
                awaitStarvation(pool);
                if (System.getenv("POOL_STARVATION_DUMP") != null) {
                    Thread.sleep(Duration.ofSeconds(15));
                }
                pool.shutdown(Duration.ZERO);
            }
            for (Future<Void> request : requests) {
                if (expectStarvation) {
                    ExecutionException failure = assertThrows(ExecutionException.class,
                            () -> request.get(5, TimeUnit.SECONDS));
                    assertInstanceOf(IllegalStateException.class, failure.getCause());
                } else {
                    request.get(5, TimeUnit.SECONDS);
                }
            }
        } finally {
            pool.shutdown(Duration.ZERO);
            executor.shutdownNow();
        }
    }

    private List<Future<Void>> submitRequests(ConnectionPool pool, ExecutorService executor) {
        CyclicBarrier firstBorrowed = new CyclicBarrier(THREADS);
        List<Future<Void>> requests = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            requests.add(executor.submit(() -> {
                try (var first = pool.borrow(Duration.ofSeconds(5))) {
                    firstBorrowed.await(5, TimeUnit.SECONDS);
                    try (var second = pool.borrow(Duration.ofMinutes(1))) {
                        second.raw();
                    }
                }
                return null;
            }));
        }
        return requests;
    }

    private void awaitStarvation(ConnectionPool pool) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        PoolStats stats = pool.stats();
        while (stats.waiting() < THREADS && System.nanoTime() < deadline) {
            Thread.sleep(Duration.ofMillis(1));
            stats = pool.stats();
        }
        assertEquals(new PoolStats(THREADS, 0, THREADS, 0, THREADS), stats);
    }
}
