package io.github.phuongtran.androidoutbox

import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.EOFException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.Channels
import kotlin.random.Random

class OutboxCodecRobustnessTest {
    @Test(timeout = 10_000L)
    fun `decoders tolerate deterministic malformed byte corpus`() {
        val random = Random(SEED)
        repeat(CORPUS_SIZE) {
            val bytes = ByteArray(random.nextInt(MAX_CORPUS_BYTES + 1))
            random.nextBytes(bytes)
            OutboxBatchFrameCodec.decode(bytes)
            OutboxControlResponseFrameCodec.decode(bytes)
        }
    }

    @Test(timeout = 10_000L)
    fun `fd frame reader rejects malformed lengths without hanging or large allocation`() {
        val random = Random(SEED xor 0x5A5A5A5A)
        repeat(FRAME_CORPUS_SIZE) { caseIndex ->
            val frame = ByteArray(random.nextInt(4, MAX_CORPUS_BYTES + 1))
            random.nextBytes(frame)
            val declaredLength = if (caseIndex % 2 == 0) {
                OutboxConfig.MAX_PIPE_FRAME_BYTES + 1 + random.nextInt(1024)
            } else {
                random.nextInt(MAX_CORPUS_BYTES + 1)
            }
            ByteBuffer
                .wrap(frame)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(declaredLength)
            val channel = Channels.newChannel(ByteArrayInputStream(frame))
            try {
                OutboxFdFrameCodec.readFrame(channel)
            } catch (_: IllegalArgumentException) {
                // Invalid or over-budget frame length is the expected safe rejection.
            } catch (_: EOFException) {
                // A valid-looking length with a truncated body is also safely rejected.
            }
        }
    }

    private companion object {
        const val SEED = 0x41_4F_42
        const val CORPUS_SIZE = 20_000
        const val FRAME_CORPUS_SIZE = 5_000
        const val MAX_CORPUS_BYTES = 512
    }
}
