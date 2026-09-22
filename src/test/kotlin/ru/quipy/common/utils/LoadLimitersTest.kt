package ru.quipy.common.utils

import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class LoadLimitersTest {
    @Test
    fun `sliding window rejects requests above the configured rate`() {
        val limiter = SlidingWindowRateLimiter(rate = 2, window = Duration.ofSeconds(1))

        assertTrue(limiter.tick())
        assertTrue(limiter.tick())
        assertFalse(limiter.tick())
    }

    @Test
    fun `blocking sliding window waits until a permit expires`() {
        val limiter = SlidingWindowRateLimiter(rate = 1, window = Duration.ofMillis(80))
        assertTrue(limiter.tick())

        assertTrue(limiter.tickBlocking(Duration.ofMillis(500)))
    }

    @Test
    fun `blocking sliding window respects its timeout`() {
        val limiter = SlidingWindowRateLimiter(rate = 1, window = Duration.ofSeconds(1))
        assertTrue(limiter.tick())

        assertFalse(limiter.tickBlocking(Duration.ofMillis(20)))
    }

    @Test
    fun `ongoing window limits parallel work`() {
        val window = OngoingWindow(maxWinSize = 1)
        window.acquire()

        assertFalse(window.acquire(Duration.ofMillis(20)))
        window.release()
        assertTrue(window.acquire(Duration.ofMillis(20)))
        window.release()
    }
}
