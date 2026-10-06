package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    registry: MeterRegistry,
) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val acceptedCounter =
        Counter.builder("payment.accepted").description("Payment tasks accepted by the shop").register(registry)
    private val completedCounter =
        Counter.builder("payment.completed").description("Payment tasks finished, including failed tasks").register(registry)

    private val paymentExecutor =
        ThreadPoolExecutor(
            16,
            16,
            0L,
            TimeUnit.MILLISECONDS,
            LinkedBlockingQueue(8_000),
            NamedThreadFactory("payment-submission-executor"),
            CallerBlockingRejectedExecutionHandler(),
        )

    init {
        Gauge.builder("payment.executor.queued", paymentExecutor) { it.queue.size.toDouble() }.register(registry)
        Gauge.builder("payment.executor.active", paymentExecutor) { it.activeCount.toDouble() }.register(registry)
    }

    fun processPayment(
        orderId: UUID,
        amount: Int,
        paymentId: UUID,
        deadline: Long,
    ): Long {
        val createdAt = System.currentTimeMillis()
        paymentExecutor.submit {
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
            } finally {
                completedCounter.increment()
            }
        }
        acceptedCounter.increment()
        return createdAt
    }
}
