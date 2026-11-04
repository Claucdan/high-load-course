package ru.quipy.common.utils

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class SlidingWindowRateLimiter(
    private val rate: Long,
    window: Duration,
    private val nanoTime: () -> Long = System::nanoTime,
) : RateLimiter {
    private val windowNanos = window.toNanos()
    private val lock = ReentrantLock(true)
    private val requests = ArrayDeque<Long>()

    init {
        require(rate > 0)
        require(windowNanos > 0)
    }

    override fun tick(): Boolean {
        if (!lock.tryLock()) return false
        try {
            return acquirePermit()
        } finally {
            lock.unlock()
        }
    }

    fun tickBlocking(timeout: Duration = Duration.ofMinutes(30)): Boolean {
        val timeoutNanos = timeout.toNanos().coerceAtLeast(0)
        val deadline = System.nanoTime() + timeoutNanos
        if (!lock.tryLock(timeoutNanos, TimeUnit.NANOSECONDS)) return false
        try {
            while (!acquirePermit()) {
                val remaining = deadline - System.nanoTime()
                if (remaining <= 0) return false
                val waitNanos = windowNanos - (nanoTime() - requests.first())
                TimeUnit.NANOSECONDS.sleep(minOf(waitNanos.coerceAtLeast(1), remaining))
            }
            return true
        } finally {
            lock.unlock()
        }
    }

    private fun acquirePermit(): Boolean {
        val now = nanoTime()
        while (requests.isNotEmpty() && now - requests.first() >= windowNanos) {
            requests.removeFirst()
        }
        if (requests.size >= rate) return false
        requests.addLast(now)
        return true
    }
}
