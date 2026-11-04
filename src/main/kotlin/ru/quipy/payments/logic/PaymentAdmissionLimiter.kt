package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.delay
import org.springframework.stereotype.Service
import ru.quipy.common.utils.LeakyBucketQueueRateLimiter
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.ceil

@Service
class PaymentAdmissionLimiter(
    accountProperties: List<PaymentAccountProperties>,
    registry: MeterRegistry,
) {
    private val processingTimeMillis = accountProperties.sumOf { it.averageProcessingTime.toMillis().coerceAtLeast(1) }
    private val processingBudgetMillis = processingTimeMillis * 2 + PAYMENT_RESPONSE_MARGIN_MILLIS
    private val rate = accountProperties.minOf { minOf(it.rateLimitPerSec.toDouble(), it.parallelRequests * 1_000.0 / processingTimeMillis) }
    private val limiter = LeakyBucketQueueRateLimiter(rate, Duration.ofSeconds(30))
    private val waiting = AtomicInteger()
    private val rejectedCounter = registry.counter("payment.rejected", "reason", "rate_limit")
    private val incomingCounter = registry.counter("payment.incoming")
    private val waitTimer = Timer.builder("payment.admission.wait").register(registry)

    init {
        Gauge.builder("payment.admission.waiting", waiting) { it.get().toDouble() }.register(registry)
        Gauge.builder("payment.admission.delay", limiter) { it.waitingTime().toNanos() / 1_000_000_000.0 }.register(registry)
        Gauge.builder("payment.admission.rate") { rate }.register(registry)
    }

    suspend fun acquire(deadline: Long): Boolean {
        incomingCounter.increment()
        val budget = Duration.ofMillis(deadline - now() - processingBudgetMillis)
        val wait = limiter.reserve(budget)
        if (wait == null) {
            rejectedCounter.increment()
            return false
        }
        waiting.incrementAndGet()
        val startedAt = System.nanoTime()
        try {
            delay(ceil(wait.toNanos() / 1_000_000.0).toLong())
            return true
        } finally {
            waiting.decrementAndGet()
            waitTimer.record(Duration.ofNanos(System.nanoTime() - startedAt))
        }
    }

    fun retryAfter(): Long = now() + maxOf(1, ceil(limiter.retryAfter().toNanos() / 1_000_000.0).toLong())
}
