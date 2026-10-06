package ru.quipy.payments.logic

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.springframework.test.util.ReflectionTestUtils
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentCreatedEvent
import java.net.SocketTimeoutException
import java.util.UUID
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class OrderPayerMetricsTest {
    private val registry = SimpleMeterRegistry()
    private val payer = OrderPayer(registry)

    @Suppress("UNCHECKED_CAST")
    private val eventService =
        Mockito.mock(EventSourcingService::class.java) as EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
    private val paymentService = Mockito.mock(PaymentService::class.java)
    private val orderId = UUID.randomUUID()
    private val paymentId = UUID.randomUUID()
    private val executor = ReflectionTestUtils.getField(payer, "paymentExecutor") as ThreadPoolExecutor

    @BeforeEach
    fun configureServices() {
        ReflectionTestUtils.setField(payer, "paymentESService", eventService)
        ReflectionTestUtils.setField(payer, "paymentService", paymentService)
        Mockito
            .doReturn(PaymentCreatedEvent(paymentId, orderId, 100))
            .`when`(eventService)
            .create<PaymentCreatedEvent>(any(), any<(PaymentAggregateState) -> PaymentCreatedEvent>())
    }

    @Test
    fun `counters start at zero and successful payment is counted once`() {
        assertCounts(0.0, 0.0)
        submitAndDrain()
        assertCounts(1.0, 1.0)
        Mockito.verify(paymentService).submitPaymentRequest(any(), Mockito.eq(100), Mockito.anyLong(), Mockito.anyLong())
    }

    @Test
    fun `failed event creation still completes the accepted task`() {
        Mockito
            .doThrow(IllegalStateException("Event store unavailable"))
            .`when`(eventService)
            .create<PaymentCreatedEvent>(any(), any<(PaymentAggregateState) -> PaymentCreatedEvent>())
        submitAndDrain()
        assertCounts(1.0, 1.0)
        Mockito.verifyNoInteractions(paymentService)
    }

    @Test
    fun `payment deadline still completes the accepted task`() {
        Mockito
            .doAnswer { throw SocketTimeoutException("Payment deadline exceeded") }
            .`when`(paymentService)
            .submitPaymentRequest(any(), Mockito.anyInt(), Mockito.anyLong(), Mockito.anyLong())
        submitAndDrain()
        assertCounts(1.0, 1.0)
    }

    @Test
    fun `rejected task is neither accepted nor completed`() {
        executor.shutdown()
        assertThrows(RejectedExecutionException::class.java) {
            payer.processPayment(orderId, 100, paymentId, System.currentTimeMillis() + 60_000)
        }
        assertCounts(0.0, 0.0)
        Mockito.verifyNoInteractions(paymentService)
    }

    private fun submitAndDrain() {
        try {
            payer.processPayment(orderId, 100, paymentId, System.currentTimeMillis() + 60_000)
        } finally {
            executor.shutdown()
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS))
        }
    }

    private fun assertCounts(
        accepted: Double,
        completed: Double,
    ) {
        assertEquals(accepted, registry.get("payment.accepted").counter().count())
        assertEquals(completed, registry.get("payment.completed").counter().count())
    }

    private fun <T> any(): T = Mockito.any<T>()
}
