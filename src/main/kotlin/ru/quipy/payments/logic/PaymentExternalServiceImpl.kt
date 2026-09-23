package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*

/**
 * Sends payments through one configured external provider account.
 *
 * The adapter applies two forms of back pressure before performing an HTTP call:
 * [OngoingWindow] limits concurrent operations and [SlidingWindowRateLimiter] limits the request
 * rate. Both waits respect the payment deadline and leave enough time for the provider's average
 * processing duration.
 *
 * @param properties limits and metadata reported for the provider account
 * @param paymentESService event-sourcing service used to persist submission and processing results
 * @param paymentProviderHostPort provider address in `host:port` form
 * @param token authentication token passed to the provider
 */
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()
    private val rateLimiter =
        SlidingWindowRateLimiter(
            rate = rateLimitPerSec.toLong(),
            window = Duration.ofMillis(1_050),
        )
    private val ongoingWindow = OngoingWindow(parallelRequests)

    /**
     * Processes a payment request while respecting concurrency, rate and deadline constraints.
     *
     * @param paymentId aggregate identifier of the payment
     * @param amount amount submitted to the provider
     * @param paymentStartedAt Unix timestamp in milliseconds when the application accepted payment
     * @param deadline absolute Unix timestamp in milliseconds by which processing should complete
     */
    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
    ) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()

        val ongoingSlotAcquired =
            try {
                ongoingWindow.acquire(queueWaitTimeout(deadline))
            } catch (exception: InterruptedException) {
                Thread.currentThread().interrupt()
                false
            }
        if (!ongoingSlotAcquired) {
            logger.warn("[$accountName] Payment $paymentId exceeded its deadline in the ongoing queue")
            paymentESService.update(paymentId) {
                it.logSubmission(
                    success = false,
                    transactionId = transactionId,
                    startedAt = now(),
                    spentInQueueDuration = Duration.ofMillis(now() - paymentStartedAt),
                )
            }
            paymentESService.update(paymentId) {
                it.logProcessing(
                    success = false,
                    processedAt = now(),
                    transactionId = transactionId,
                    reason = "Payment deadline exceeded while waiting for ongoing request slot.",
                )
            }
            return
        }

        try {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            val ratePermitAcquired =
                try {
                    rateLimiter.tickBlocking(queueWaitTimeout(deadline))
                } catch (exception: InterruptedException) {
                    Thread.currentThread().interrupt()
                    false
                }
            if (!ratePermitAcquired) {
                logger.warn("[$accountName] Payment $paymentId exceeded its deadline in the rate-limit queue")
                paymentESService.update(paymentId) {
                    it.logProcessing(
                        success = false,
                        processedAt = now(),
                        transactionId = transactionId,
                        reason = "Payment deadline exceeded while waiting for rate-limit permit.",
                    )
                }
                return
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            try {
                val request =
                    Request
                        .Builder()
                        .run {
                            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                            post(emptyBody)
                        }.build()

                client.newCall(request).execute().use { response ->
                    val body =
                        try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            }
        } finally {
            ongoingWindow.release()
        }
    }

    /** Returns the configured cost of using this provider account. */
    override fun price() = properties.price

    /** Returns whether this provider account is available for payment routing. */
    override fun isEnabled() = properties.enabled

    /** Returns the unique provider account name. */
    override fun name() = properties.accountName

    private fun queueWaitTimeout(deadline: Long): Duration =
        Duration.ofMillis(
            (deadline - now() - requestAverageProcessingTime.toMillis()).coerceAtLeast(0),
        )
}

/** Returns the current wall-clock time as a Unix timestamp in milliseconds. */
public fun now() = System.currentTimeMillis()
