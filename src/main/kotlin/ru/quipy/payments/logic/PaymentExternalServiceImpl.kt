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
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val registry: MeterRegistry,
) : PaymentExternalSystemAdapter {
    companion object {
        private const val RESPONSE_MARGIN_MILLIS = 1_000L

        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec

    private val requestIntervalNanos = TimeUnit.SECONDS.toNanos(1) / rateLimitPerSec * 105 / 100
    private var nextRequestAtNanos = System.nanoTime()
    private val client =
        OkHttpClient
            .Builder()
            .readTimeout(Duration.ofSeconds(15))
            .retryOnConnectionFailure(false)
            .addNetworkInterceptor { chain ->
                val request = chain.request()
                val deadline = request.tag(Long::class.javaObjectType)!!
                waitForRequestRateLimit(deadline)
                val providerTimeout = Duration.ofMillis(remainingProviderTime(deadline))
                val url =
                    request.url
                        .newBuilder()
                        .addQueryParameter("timeout", providerTimeout.toString())
                        .build()
                chain.proceed(request.newBuilder().url(url).build())
            }.build()
    private val requestWindow = Semaphore(properties.parallelRequests, true)

    private val waitTimer = Timer.builder("payment.wait").tag("account", accountName).register(registry)

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
    }

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
    ) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        var windowAcquired = false
        var outcome = "error"
        var requestSample: Timer.Sample? = null
        try {
            val remainingTime = deadline - now()
            if (remainingTime <= 0 || !requestWindow.tryAcquire(remainingTime, TimeUnit.MILLISECONDS)) {
                throw SocketTimeoutException("Payment deadline exceeded while waiting for request window.")
            }
            windowAcquired = true

            val request =
                Request
                    .Builder()
                    .run {
                        url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                        post(emptyBody)
                        tag(Long::class.javaObjectType, deadline)
                    }.build()

            waitTimer.record(maxOf(0, now() - paymentStartedAt), TimeUnit.MILLISECONDS)
            requestSample = Timer.start(registry)
            val remainingCallTime = deadline - now()
            remainingProviderTime(deadline)
            val call = client.newCall(request)
            call.timeout().timeout(remainingCallTime, TimeUnit.MILLISECONDS)
            call.execute().use { response ->
                val body =
                    try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                requestWindow.release()
                windowAcquired = false
                outcome =
                    when {
                        body.result -> "success"
                        response.code == 408 -> "deadline"
                        else -> "provider_rejected"
                    }
                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is InterruptedIOException -> {
                    outcome = if (deadline - now() <= RESPONSE_MARGIN_MILLIS) "deadline" else "timeout"
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
        } finally {
            if (windowAcquired) requestWindow.release()
            registry.counter("payment.requests", "account", accountName, "outcome", outcome).increment()
            requestSample?.stop(Timer.builder("payment.provider.duration").tags("account", accountName, "outcome", outcome).register(registry))
            if (requestSample == null) waitTimer.record(maxOf(0, now() - paymentStartedAt), TimeUnit.MILLISECONDS)
        }
    }

    @Synchronized
    private fun waitForRequestRateLimit(deadline: Long) {
        var waitNanos = nextRequestAtNanos - System.nanoTime()
        while (waitNanos > 0) {
            val remainingTime = remainingProviderTime(deadline)
            TimeUnit.NANOSECONDS.sleep(minOf(waitNanos, TimeUnit.MILLISECONDS.toNanos(remainingTime)))
            waitNanos = nextRequestAtNanos - System.nanoTime()
        }
        remainingProviderTime(deadline)
        nextRequestAtNanos = System.nanoTime() + requestIntervalNanos
    }

    private fun remainingProviderTime(deadline: Long): Long {
        val remaining = deadline - now() - RESPONSE_MARGIN_MILLIS
        if (remaining <= 0) throw SocketTimeoutException("Insufficient payment deadline budget to send request and receive its result.")
        return remaining
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()
