package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Limits the number of permits granted during a rolling time window.
 *
 * Unlike a fixed-window limiter, this implementation cannot produce a burst at the boundary
 * between two adjacent windows. Blocking calls sleep until the oldest permit expires instead of
 * polling in a loop.
 */
class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    private val windowNanos = window.toNanos()
    private val lock = ReentrantLock(true)
    private val permitAvailable = lock.newCondition()
    private val grantedAt = ArrayDeque<Long>()

    init {
        require(rate > 0) { "Rate must be positive" }
        require(!window.isZero && !window.isNegative) { "Window must be positive" }
    }

    override fun tick(): Boolean =
        lock.withLock {
            val now = System.nanoTime()
            evictExpired(now)
            if (grantedAt.size >= rate) {
                false
            } else {
                grantedAt.addLast(now)
                true
            }
        }

    @Throws(InterruptedException::class)
    fun tickBlocking() {
        lock.lockInterruptibly()
        try {
            while (!tryAcquire(System.nanoTime())) {
                permitAvailable.awaitNanos(nanosUntilNextPermit(System.nanoTime()))
            }
        } finally {
            lock.unlock()
        }
    }

    /**
     * Waits for a permit for no longer than [timeout]. Returns `false` when the timeout expires.
     */
    @Throws(InterruptedException::class)
    fun tickBlocking(timeout: Duration): Boolean {
        if (timeout.isNegative) return false

        var remainingNanos = timeout.toNanos()
        lock.lockInterruptibly()
        try {
            while (!tryAcquire(System.nanoTime())) {
                if (remainingNanos <= 0) return false

                val waitNanos = minOf(remainingNanos, nanosUntilNextPermit(System.nanoTime()))
                remainingNanos -= waitNanos - permitAvailable.awaitNanos(waitNanos)
            }
            return true
        } finally {
            lock.unlock()
        }
    }

    private fun tryAcquire(now: Long): Boolean {
        evictExpired(now)
        if (grantedAt.size >= rate) return false

        grantedAt.addLast(now)
        return true
    }

    private fun evictExpired(now: Long) {
        while (grantedAt.isNotEmpty() && now - grantedAt.first() >= windowNanos) {
            grantedAt.removeFirst()
            permitAvailable.signalAll()
        }
    }

    private fun nanosUntilNextPermit(now: Long): Long {
        val oldestPermit = grantedAt.firstOrNull() ?: return 1
        return (oldestPermit + windowNanos - now).coerceAtLeast(1)
    }
}
