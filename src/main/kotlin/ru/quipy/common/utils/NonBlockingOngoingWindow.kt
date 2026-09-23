package ru.quipy.common.utils

import java.time.Duration
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Blocking concurrency gate backed by a [Semaphore].
 *
 * A slot represents one operation that is currently in progress. Every successful [acquire] must
 * be paired with exactly one [release], normally in a `finally` block.
 *
 * @param maxWinSize maximum number of operations allowed to run concurrently
 */
class OngoingWindow(
    maxWinSize: Int,
) {
    private val window = Semaphore(maxWinSize)

    /**
     * Blocks until a concurrency slot becomes available.
     *
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    @Throws(InterruptedException::class)
    fun acquire() {
        window.acquire()
    }

    /**
     * Attempts to acquire a concurrency slot within [timeout].
     *
     * @param timeout maximum time the calling thread may wait
     * @return `true` when a slot was acquired, or `false` when [timeout] expired
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    @Throws(InterruptedException::class)
    fun acquire(timeout: Duration): Boolean {
        if (timeout.isNegative) return false
        return window.tryAcquire(timeout.toNanos(), TimeUnit.NANOSECONDS)
    }

    /** Releases one previously acquired concurrency slot. */
    fun release() = window.release()

    /**
     * Returns an approximate snapshot of the number of threads waiting for a slot.
     */
    fun awaitingQueueSize() = window.queueLength
}

/**
 * Lock-free concurrency gate for callers that must not block.
 *
 * @param maxWinSize maximum number of operations allowed inside the window
 */
class NonBlockingOngoingWindow(
    private val maxWinSize: Int,
) {
    private val winSize = AtomicInteger()

    /**
     * Attempts to reserve a slot immediately.
     *
     * @return [WindowResponse.Success] when admitted, otherwise [WindowResponse.Fail]
     */
    fun putIntoWindow(): WindowResponse {
        while (true) {
            val currentWinSize = winSize.get()
            if (currentWinSize >= maxWinSize) {
                return WindowResponse.Fail(currentWinSize)
            }

            if (winSize.compareAndSet(currentWinSize, currentWinSize + 1)) {
                break
            }
        }
        return WindowResponse.Success(winSize.get())
    }

    /** Releases one slot previously reserved by [putIntoWindow]. */
    fun releaseWindow() = winSize.decrementAndGet()

    /** Result of a non-blocking attempt to reserve a concurrency slot. */
    sealed class WindowResponse(
        /** Number of occupied slots observed after the attempt. */
        val currentWinSize: Int,
    ) {
        /** Indicates that the caller was admitted to the concurrency window. */
        public class Success(
            currentWinSize: Int,
        ) : WindowResponse(currentWinSize)

        /** Indicates that the concurrency window was already full. */
        public class Fail(
            currentWinSize: Int,
        ) : WindowResponse(currentWinSize)
    }
}
