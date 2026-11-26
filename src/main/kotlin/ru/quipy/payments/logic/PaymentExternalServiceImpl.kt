package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

internal const val PAYMENT_RESPONSE_MARGIN_MILLIS = 200L

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val registry: MeterRegistry,
    attemptTimeout: Duration? = null,
) : PaymentExternalSystemAdapter {
    companion object {
        private const val MAX_ATTEMPTS = 3

        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val attemptTimeoutMillis = attemptTimeout?.toMillis()?.also { require(it > 0) }
    private val retryProcessingMillis = minOf(properties.averageProcessingTime.toMillis(), attemptTimeoutMillis ?: Long.MAX_VALUE)
    private val retryCounter = registry.counter("payment.retries", "account", accountName)
    private val retryDeadlineCounter = registry.counter("payment.retry.skipped", "account", accountName, "reason", "deadline")

    private val requestRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1).plusMillis(1))
    private val client =
        OkHttpClient
            .Builder()
            .readTimeout(Duration.ofSeconds(15))
            .retryOnConnectionFailure(false)
            .addNetworkInterceptor { chain ->
                val request = chain.request()
                val attempt = request.tag(PaymentRequestContext::class.java)!!
                val deadline = attempt.deadline
                waitForRequestRateLimit(deadline)
                val remaining = remainingProviderTime(deadline)
                if (attemptTimeoutMillis != null && attempt.retry && remaining < retryProcessingMillis) {
                    retryDeadlineCounter.increment()
                    throw SocketTimeoutException("Insufficient payment deadline budget for another attempt.")
                }
                val providerTimeout = Duration.ofMillis(minOf(remaining, attemptTimeoutMillis ?: remaining))
                val url =
                    request.url
                        .newBuilder()
                        .addQueryParameter("timeout", providerTimeout.toString())
                        .build()
                if (attempt.retry) retryCounter.increment()
                chain.proceed(request.newBuilder().url(url).build())
            }.build()
    private val requestWindow = Semaphore(properties.parallelRequests, true)

    private val waitTimer = Timer.builder("payment.wait").tag("account", accountName).register(registry)
    private val paymentTimer =
        Timer
            .builder("payment.duration")
            .tag("account", accountName)
            .publishPercentiles(0.95)
            .register(registry)
    private val recoveredCounter = registry.counter("payment.recovered", "account", accountName)

    init {
        Gauge
            .builder("payment.requests.active", requestWindow) {
                (properties.parallelRequests - it.availablePermits()).toDouble()
            }.tag("account", accountName)
            .register(registry)
        Gauge
            .builder("payment.requests.waiting", requestWindow) { it.queueLength.toDouble() }
            .tag("account", accountName)
            .register(registry)
        listOf("success", "temporary_error", "rate_limit", "provider_rejected", "provider_timeout", "deadline", "timeout", "error").forEach {
            registry.counter("payment.requests", "account", accountName, "outcome", it)
        }
        listOf("success", "failed").forEach {
            registry.counter("payment.results", "account", accountName, "outcome", it)
        }
    }

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
    ) {
        var success = false
        var retryDelayMillis = 0L
        try {
            repeat(MAX_ATTEMPTS) { attempt ->
                if (attempt > 0) {
                    val processingBudget = retryProcessingMillis + PAYMENT_RESPONSE_MARGIN_MILLIS
                    if (deadline - now() <= processingBudget + retryDelayMillis) {
                        retryDeadlineCounter.increment()
                        return
                    }
                    if (retryDelayMillis > 0) TimeUnit.MILLISECONDS.sleep(retryDelayMillis)
                }

                val result = performAttempt(paymentId, amount, paymentStartedAt, deadline, retry = attempt > 0)
                if (result.success) {
                    success = true
                    if (attempt > 0) recoveredCounter.increment()
                    return
                }
                if (!result.retryable) return
                retryDelayMillis = result.retryDelayMillis
            }
        } finally {
            registry.counter("payment.results", "account", accountName, "outcome", if (success) "success" else "failed").increment()
            paymentTimer.record(maxOf(0, now() - paymentStartedAt), TimeUnit.MILLISECONDS)
        }
    }

    private fun performAttempt(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        retry: Boolean,
    ): PaymentAttemptResult {
        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId, txId: $transactionId")

        var windowAcquired = false
        var outcome = "error"
        var requestSample: Timer.Sample? = null
        try {
            val remainingTime = remainingProviderTime(deadline)
            if (!requestWindow.tryAcquire(remainingTime, TimeUnit.MILLISECONDS)) {
                throw SocketTimeoutException("Payment deadline exceeded while waiting for request window.")
            }
            windowAcquired = true

            val request =
                Request
                    .Builder()
                    .run {
                        url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                        post(emptyBody)
                        tag(PaymentRequestContext::class.java, PaymentRequestContext(deadline, retry))
                    }.build()

            waitTimer.record(maxOf(0, now() - paymentStartedAt), TimeUnit.MILLISECONDS)
            requestSample = Timer.start(registry)
            val remainingCallTime = deadline - now()
            remainingProviderTime(deadline)
            val call = client.newCall(request)
            call.timeout().timeout(remainingCallTime, TimeUnit.MILLISECONDS)
            return call.execute().use { response ->
                val responseBody = response.body?.string()
                val body =
                    try {
                        mapper.readValue(responseBody, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Invalid response for txId: $transactionId, payment: $paymentId, result code: ${response.code}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                requestWindow.release()
                windowAcquired = false
                val success = response.isSuccessful && body.result
                val confirmedTimeout =
                    response.code == 408 &&
                        !body.result && body.message == "Timeout" &&
                        body.transactionId == transactionId.toString() && body.paymentId == paymentId.toString()
                if (response.code == 408) {
                    registry
                        .timer("payment.provider.timeout.remaining", "account", accountName, "confirmed", confirmedTimeout.toString())
                        .record(maxOf(0, deadline - now()), TimeUnit.MILLISECONDS)
                }
                val retryableTimeout = confirmedTimeout && attemptTimeoutMillis != null
                val retryable =
                    !success &&
                        (retryableTimeout || response.code in setOf(429, 500, 502, 503, 504) || response.isSuccessful && body.message == "Temporary error")
                outcome =
                    when {
                        success -> "success"
                        retryableTimeout -> "provider_timeout"
                        response.code == 408 -> "deadline"
                        response.code == 429 -> "rate_limit"
                        retryable -> "temporary_error"
                        else -> "provider_rejected"
                    }
                logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: $success, message: ${body.message}")

                paymentESService.update(paymentId) {
                    it.logProcessing(success, now(), transactionId, reason = body.message)
                }
                val retryDelayMillis =
                    response.header("Retry-After")?.toLongOrNull()?.let { TimeUnit.SECONDS.toMillis(it.coerceAtLeast(0)) }
                        ?: if (response.code == 429) 100L else 0L
                PaymentAttemptResult(success, retryable, retryDelayMillis)
            }
        } catch (e: Exception) {
            if (e is InterruptedException) Thread.currentThread().interrupt()
            if (e is InterruptedIOException) {
                outcome = if (deadline - now() <= PAYMENT_RESPONSE_MARGIN_MILLIS) "deadline" else "timeout"
            }
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = e.message)
            }
            return PaymentAttemptResult(false, false)
        } finally {
            if (windowAcquired) requestWindow.release()
            registry.counter("payment.requests", "account", accountName, "outcome", outcome).increment()
            requestSample?.stop(Timer.builder("payment.provider.duration").tags("account", accountName, "outcome", outcome).register(registry))
            if (requestSample == null) waitTimer.record(maxOf(0, now() - paymentStartedAt), TimeUnit.MILLISECONDS)
        }
    }

    private fun waitForRequestRateLimit(deadline: Long) {
        if (!requestRateLimiter.tickBlocking(Duration.ofMillis(remainingProviderTime(deadline)))) {
            throw SocketTimeoutException("Payment deadline exceeded while waiting for request rate limit.")
        }
    }

    private fun remainingProviderTime(deadline: Long): Long {
        val remaining = deadline - now() - PAYMENT_RESPONSE_MARGIN_MILLIS
        if (remaining <= 0) throw SocketTimeoutException("Insufficient payment deadline budget to send request and receive its result.")
        return remaining
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private data class PaymentRequestContext(
        val deadline: Long,
        val retry: Boolean,
    )

    private data class PaymentAttemptResult(
        val success: Boolean,
        val retryable: Boolean,
        val retryDelayMillis: Long = 0,
    )
}

public fun now() = System.currentTimeMillis()
