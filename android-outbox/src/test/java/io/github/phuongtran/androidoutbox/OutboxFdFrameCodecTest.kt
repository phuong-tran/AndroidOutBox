package io.github.phuongtran.androidoutbox

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Test
import java.io.EOFException
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel

class OutboxFdFrameCodecTest {

    @Test
    fun `writeFrame should write big endian length prefix`() {
        val file = tempFrameFile()

        FileOutputStream(file).channel.use { channel ->
            OutboxFdFrameCodec.writeFrame(
                outputChannel = channel,
                payload = byteArrayOf(1, 2, 3, 4),
            )
        }

        assertArrayEquals(
            byteArrayOf(0, 0, 0, 4, 1, 2, 3, 4),
            file.readBytes(),
        )
    }

    @Test
    fun `readFrame should read payload from length prefixed stream`() {
        val file = tempFrameFile(byteArrayOf(0, 0, 0, 3, 9, 8, 7))

        FileInputStream(file).channel.use { channel ->
            assertArrayEquals(
                byteArrayOf(9, 8, 7),
                OutboxFdFrameCodec.readFrame(channel),
            )
        }
    }

    @Test
    fun `readFrame should round trip megabyte payload`() {
        val payload = ByteArray(MEGABYTE) { index -> (index % Byte.MAX_VALUE).toByte() }
        val file = tempFrameFile()

        FileOutputStream(file).channel.use { channel ->
            OutboxFdFrameCodec.writeFrame(
                outputChannel = channel,
                payload = payload,
            )
        }

        FileInputStream(file).channel.use { channel ->
            assertArrayEquals(
                payload,
                OutboxFdFrameCodec.readFrame(channel),
            )
        }
    }

    @Test
    fun `readFrame should return null when stream is closed before header`() {
        FileInputStream(tempFrameFile()).channel.use { channel ->
            assertNull(OutboxFdFrameCodec.readFrame(channel))
        }
    }

    @Test
    fun `readFrame should throw when stream closes during payload`() {
        val file = tempFrameFile(byteArrayOf(0, 0, 0, 4, 1, 2))

        assertThrows(EOFException::class.java) {
            FileInputStream(file).channel.use { channel ->
                OutboxFdFrameCodec.readFrame(channel)
            }
        }
    }

    @Test
    fun `readFrame should throw when stream makes no progress`() {
        assertThrows(EOFException::class.java) {
            OutboxFdFrameCodec.readFrame(ZeroReadChannel())
        }
    }

    @Test
    fun `readFrame should reject invalid negative frame size`() {
        val file = tempFrameFile(byteArrayOf(-1, -1, -1, -1))

        assertThrows(IllegalArgumentException::class.java) {
            FileInputStream(file).channel.use { channel ->
                OutboxFdFrameCodec.readFrame(channel)
            }
        }
    }

    private fun tempFrameFile(bytes: ByteArray = ByteArray(0)): File {
        return File.createTempFile("observability-frame", ".bin").apply {
            deleteOnExit()
            writeBytes(bytes)
        }
    }

    private class ZeroReadChannel : ReadableByteChannel {
        override fun read(dst: ByteBuffer): Int = 0

        override fun isOpen(): Boolean = true

        override fun close() = Unit
    }

    private companion object {
        const val MEGABYTE = 1024 * 1024
    }
}
