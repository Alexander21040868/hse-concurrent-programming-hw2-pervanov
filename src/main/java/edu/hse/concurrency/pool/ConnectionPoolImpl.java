package edu.hse.concurrency.pool;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionPoolImpl implements ConnectionPool {

    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition idleChanged = lock.newCondition();
    private final Condition drained = lock.newCondition();
    private final ArrayDeque<Idle> idle = new ArrayDeque<>();
    private final Set<Lease> borrowed = new HashSet<>();
    private final LinkedHashSet<Condition> waiters = new LinkedHashSet<>();
    private final ConnectionFactory factory;
    private final long idleTimeout;
    private final Thread reaper;

    private PoolConfig config;
    private int creating;
    private int closing;
    private boolean stopped;

    public ConnectionPoolImpl(PoolConfig config, ConnectionFactory factory) {
        this.config = Objects.requireNonNull(config, "config");
        this.factory = Objects.requireNonNull(factory, "factory");
        idleTimeout = nanos(config.idleTimeout());
        reaper = new Thread(this::reapIdle, "pool-idle-reaper");
        reaper.setDaemon(true);
        reaper.start();
    }

    @Override
    public PooledConnection borrow(Duration timeout)
            throws InterruptedException, PoolTimeoutException, ConnectionCreationException {
        long waitNanos = nanos(timeout);
        long started = System.nanoTime();
        lock.lock();
        try {
            checkRunning();
            if (!available() || config.fair() && !waiters.isEmpty()) {
                awaitTurn(timeout, waitNanos, started);
            }
            if (!idle.isEmpty()) {
                return issue(idle.removeFirst().raw());
            }
            creating++;
        } finally {
            signalBorrower();
            lock.unlock();
        }
        return createConnection();
    }

    private void awaitTurn(Duration timeout, long waitNanos, long started)
            throws InterruptedException, PoolTimeoutException {
        if (waitNanos == 0) {
            throw new PoolTimeoutException(timeout);
        }
        Condition turn = lock.newCondition();
        waiters.add(turn);
        try {
            long remaining = waitNanos - (System.nanoTime() - started);
            while (remaining > 0) {
                turn.awaitNanos(remaining);
                checkRunning();
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                remaining = waitNanos - (System.nanoTime() - started);
                if (remaining > 0 && waiters.getFirst() == turn && available()) {
                    return;
                }
            }
            throw new PoolTimeoutException(timeout);
        } finally {
            waiters.remove(turn);
        }
    }

    private PooledConnection createConnection() throws ConnectionCreationException {
        RawConnection raw;
        try {
            raw = Objects.requireNonNull(factory.create(), "connection");
        } catch (Exception e) {
            lock.lock();
            try {
                creating--;
                checkRunning();
                signalBorrower();
            } finally {
                lock.unlock();
            }
            throw new ConnectionCreationException(e);
        }
        lock.lock();
        try {
            creating--;
            if (!stopped) {
                return issue(raw);
            }
            closing++;
        } finally {
            lock.unlock();
        }
        closeConnections(List.of(raw));
        throw new IllegalStateException("Пул остановлен");
    }

    @Override
    public void resize(int maxSize) {
        List<RawConnection> excess;
        lock.lock();
        try {
            PoolConfig resized = config.withMaxSize(maxSize);
            checkRunning();
            config = resized;
            excess = removeIdle(Math.max(0, maxSize - borrowed.size() - creating), 0);
            signalBorrower();
        } finally {
            lock.unlock();
        }
        closeConnections(excess);
    }

    @Override
    public PoolConfig config() {
        lock.lock();
        try {
            return config;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PoolStats stats() {
        lock.lock();
        try {
            return new PoolStats(total(), idle.size(), borrowed.size(), creating, waiters.size());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown(Duration grace) throws InterruptedException {
        long graceNanos = nanos(grace);
        long started;
        List<RawConnection> free;
        lock.lock();
        try {
            if (stopped) {
                return;
            }
            stopped = true;
            started = System.nanoTime();
            waiters.forEach(Condition::signal);
            idleChanged.signal();
            free = removeIdle(0, 0);
        } finally {
            lock.unlock();
        }
        closeConnections(free);

        InterruptedException interrupted = null;
        List<RawConnection> forced = new ArrayList<>();
        lock.lock();
        try {
            try {
                long remaining = graceNanos - (System.nanoTime() - started);
                while (!borrowed.isEmpty() && remaining > 0) {
                    drained.awaitNanos(remaining);
                    remaining = graceNanos - (System.nanoTime() - started);
                }
            } catch (InterruptedException e) {
                interrupted = e;
            }
            for (Lease lease : borrowed) {
                forced.add(lease.raw);
            }
            closing += forced.size();
            borrowed.clear();
        } finally {
            lock.unlock();
        }
        closeConnections(forced);

        while (reaper.isAlive()) {
            try {
                reaper.join();
            } catch (InterruptedException e) {
                interrupted = e;
            }
        }
        lock.lock();
        try {
            while (closing > 0) {
                drained.awaitUninterruptibly();
            }
        } finally {
            lock.unlock();
        }
        if (Thread.interrupted() && interrupted == null) {
            interrupted = new InterruptedException();
        }
        if (interrupted != null) {
            throw interrupted;
        }
    }

    private void reapIdle() {
        while (true) {
            List<RawConnection> expired;
            lock.lock();
            try {
                while (!stopped) {
                    if (idle.size() <= config.minIdle()) {
                        idleChanged.awaitUninterruptibly();
                        continue;
                    }
                    long remaining = idleTimeout - (System.nanoTime() - idle.getFirst().since());
                    if (remaining <= 0) {
                        break;
                    }
                    try {
                        idleChanged.awaitNanos(remaining);
                    } catch (InterruptedException ignored) {
                    }
                }
                if (stopped) {
                    return;
                }
                expired = removeIdle(config.minIdle(), idleTimeout);
                signalBorrower();
            } finally {
                lock.unlock();
            }
            closeConnections(expired);
        }
    }

    private List<RawConnection> removeIdle(int keep, long minAge) {
        List<RawConnection> removed = new ArrayList<>();
        long now = System.nanoTime();
        while (idle.size() > keep && now - idle.getFirst().since() >= minAge) {
            removed.add(idle.removeFirst().raw());
        }
        closing += removed.size();
        return removed;
    }

    private void closeConnections(List<RawConnection> connections) {
        if (connections.isEmpty()) {
            return;
        }
        try {
            for (RawConnection raw : connections) {
                try {
                    raw.close();
                } catch (Exception ignored) {
                }
            }
        } finally {
            lock.lock();
            try {
                closing -= connections.size();
                if (stopped && closing == 0) {
                    drained.signal();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private int total() {
        return idle.size() + borrowed.size() + creating;
    }

    private boolean available() {
        return !idle.isEmpty() || total() < config.maxSize();
    }

    private void signalBorrower() {
        if (!stopped && !waiters.isEmpty() && available()) {
            waiters.getFirst().signal();
        }
    }

    private void checkRunning() {
        if (stopped) {
            throw new IllegalStateException("Пул остановлен");
        }
    }

    private Lease issue(RawConnection raw) {
        Lease lease = new Lease(raw);
        borrowed.add(lease);
        return lease;
    }

    private static long nanos(Duration duration) {
        if (duration.isNegative()) {
            throw new IllegalArgumentException("Длительность не может быть отрицательной");
        }
        try {
            return duration.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private record Idle(RawConnection raw, long since) {
    }

    private final class Lease implements PooledConnection {

        private final RawConnection raw;

        private Lease(RawConnection raw) {
            this.raw = raw;
        }

        @Override
        public RawConnection raw() {
            lock.lock();
            try {
                if (!borrowed.contains(this)) {
                    throw new IllegalStateException("Соединение уже возвращено или закрыто");
                }
                return raw;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() {
            lock.lock();
            try {
                if (!borrowed.remove(this)) {
                    return;
                }
                if (!stopped && total() < config.maxSize()) {
                    idle.addLast(new Idle(raw, System.nanoTime()));
                    signalBorrower();
                    idleChanged.signal();
                    return;
                }
                closing++;
                if (stopped && borrowed.isEmpty()) {
                    drained.signal();
                }
                signalBorrower();
            } finally {
                lock.unlock();
            }
            closeConnections(List.of(raw));
        }
    }
}
