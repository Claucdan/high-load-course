package ru.quipy.common.utils

import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.math.ceil

class LeakyBucketQueueRateLimiter(
    rate: Double,
    private val maxWait: Duration,
    private val nanoTime: () -> Long = System::nanoTime,
) {
    private val intervalNanos = ceil(TimeUnit.SECONDS.toNanos(1) / rate).toLong()
    private var nextRequestAtNanos = nanoTime()

    init {
        require(rate > 0 && rate.isFinite())
        require(!maxWait.isNegative)
    }

    @Synchronized
    fun reserve(waitBudget: Duration): Duration? {
        val now = nanoTime()
        val waitNanos = maxOf(0, nextRequestAtNanos - now)
        if (waitNanos > minOf(waitBudget, maxWait).toNanos()) return null
        nextRequestAtNanos = now + waitNanos + intervalNanos
        return Duration.ofNanos(waitNanos)
    }

    @Synchronized
    fun retryAfter(): Duration = Duration.ofNanos(maxOf(0, nextRequestAtNanos - nanoTime()).coerceAtMost(intervalNanos))

    @Synchronized
    fun waitingTime(): Duration = Duration.ofNanos(maxOf(0, nextRequestAtNanos - nanoTime() - intervalNanos))
}
