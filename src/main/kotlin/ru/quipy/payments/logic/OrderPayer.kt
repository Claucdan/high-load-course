package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.math.ceil

@Service
class OrderPayer(
    registry: MeterRegistry,
    accountProperties: List<PaymentAccountProperties>,
) {
    companion object {
        private const val QUEUE_WAIT_MILLIS = 500L

        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val acceptedCounter = registry.counter("payment.accepted")
    private val completedCounter = registry.counter("payment.completed")
    private val queueRejectedCounter = registry.counter("payment.rejected", "reason", "queue_full")
    private val deadlineRejectedCounter = registry.counter("payment.rejected", "reason", "deadline")
    private val processingTimeMillis = accountProperties.sumOf { it.averageProcessingTime.toMillis().coerceAtLeast(1) }
    private val requestRate = accountProperties.minOf { it.rateLimitPerSec / 1.05 }
    private val workerCount =
        minOf(
            accountProperties.maxOf { it.parallelRequests },
            maxOf(1, ceil(requestRate * processingTimeMillis / 1_000).toInt()),
        )
    private val processingRate =
        minOf(
            requestRate,
            workerCount * 1_000.0 / processingTimeMillis,
        )
    private val queueCapacity = maxOf(1, (processingRate * QUEUE_WAIT_MILLIS / 1_000).toInt())

    private val paymentExecutor =
        ThreadPoolExecutor(
            workerCount,
            workerCount,
            0L,
            TimeUnit.MILLISECONDS,
            LinkedBlockingQueue(queueCapacity),
            NamedThreadFactory("payment-submission-executor"),
            ThreadPoolExecutor.AbortPolicy(),
        )

    init {
        Gauge.builder("payment.executor.queued", paymentExecutor) { it.queue.size.toDouble() }.register(registry)
        Gauge.builder("payment.executor.active", paymentExecutor) { it.activeCount.toDouble() }.register(registry)
        Gauge.builder("payment.executor.capacity", paymentExecutor) { queueCapacity.toDouble() }.register(registry)
        logger.info("Payment executor: workers=$workerCount, queue capacity=$queueCapacity, processing rate=$processingRate")
    }

    fun processPayment(
        orderId: UUID,
        amount: Int,
        paymentId: UUID,
        deadline: Long,
    ): Long? {
        val createdAt = System.currentTimeMillis()
        val queuedPayments = paymentExecutor.queue.size + 1
        val processingBudget = ceil(queuedPayments * 1_000.0 / processingRate).toLong() + processingTimeMillis + PAYMENT_RESPONSE_MARGIN_MILLIS
        if (deadline - createdAt <= processingBudget) {
            deadlineRejectedCounter.increment()
            return null
        }

        try {
            paymentExecutor.execute {
                try {
                    val createdEvent =
                        paymentESService.create {
                            it.create(
                                paymentId,
                                orderId,
                                amount,
                            )
                        }
                    logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                    paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                } catch (e: Exception) {
                    logger.error("Payment $paymentId for order $orderId failed.", e)
                } finally {
                    completedCounter.increment()
                }
            }
        } catch (e: RejectedExecutionException) {
            queueRejectedCounter.increment()
            return null
        }
        acceptedCounter.increment()
        return createdAt
    }
}
