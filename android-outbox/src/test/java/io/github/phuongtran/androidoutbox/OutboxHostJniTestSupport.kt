@file:Suppress("TestFileNamingAndPackage")

package io.github.phuongtran.androidoutbox

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeTrue
import java.io.File
import java.io.FileDescriptor
import java.io.FileInputStream
import java.io.FileOutputStream
import java.lang.reflect.Field
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files

internal fun <T> withHostClient(
    block: (OutboxNativeControlClient, File) -> T,
): T {
    assumeTrue(HostOutboxNativeBridge.load())
    val spoolDirectory = Files
        .createTempDirectory("android-outbox-host-jni")
        .toFile()
    val transport = HostOutboxPipeTransport.adopt(
        HostOutboxNativeBridge.nativeOpenPipes(),
    )
    val client = OutboxNativeControlClient(transport)

    try {
        assertEquals(
            OutboxDoorbellEvent.HANDSHAKE,
            client.readNextDoorbell(),
        )
        return block(client, spoolDirectory)
    } finally {
        client.stopNativeOutbox()
        client.closeNativePipes()
        client.close()
        // Test artifacts must not leak into /tmp; keep cleanup quiet unless
        // deletion fails, in which case the JVM removes it on exit.
        val deleted = runCatching {
            spoolDirectory.deleteRecursively()
        }.getOrDefault(false)
        if (!deleted) {
            spoolDirectory.deleteOnExit()
        }
    }
}

internal fun configureHostOutbox(
    client: OutboxNativeControlClient,
    spoolDirectory: File,
    defaultProviderId: String,
) {
    assertTrue(
        client.configure(
            OutboxConfig(
                spoolDirectoryPath = spoolDirectory.absolutePath,
                defaultProviderId = defaultProviderId,
                queueCapacity = 8,
                maxRecordBytes = 1024,
                maxSegmentSizeBytes = 4096L,
                maxArchivedSegments = 1,
            ),
        ),
    )
}

private class HostOutboxPipeTransport private constructor(
    private val commandOutputStream: FileOutputStream,
    private val doorbellInputStream: FileInputStream,
    private val recordInputStream: FileInputStream,
) : OutboxPipeTransport {
    private val commandOutputChannel: FileChannel = commandOutputStream.channel
    private val doorbellInputChannel: FileChannel = doorbellInputStream.channel
    private val recordInputChannel: FileChannel = recordInputStream.channel
    private val commandHeaderBuffer = OutboxFdFrameCodec.newHeaderBuffer()
    private val doorbellHeaderBuffer = OutboxFdFrameCodec.newHeaderBuffer()
    private val recordHeaderBuffer = OutboxFdFrameCodec.newHeaderBuffer()

    override fun writeCommandEnvelope(envelope: ByteBuffer): Boolean {
        return runCatching {
            synchronized(commandOutputChannel) {
                OutboxFdFrameCodec.writeFrame(
                    outputChannel = commandOutputChannel,
                    payload = envelope.duplicate(),
                    headerBuffer = commandHeaderBuffer,
                )
            }
            true
        }.getOrDefault(false)
    }

    override fun readDoorbellFrame(): ByteArray? {
        return runCatching {
            OutboxFdFrameCodec.readFrame(
                inputChannel = doorbellInputChannel,
                headerBuffer = doorbellHeaderBuffer,
            )
        }.getOrNull()
    }

    override fun readControlResponseFrame(): ByteArray? {
        return runCatching {
            OutboxFdFrameCodec.readFrame(
                inputChannel = recordInputChannel,
                headerBuffer = recordHeaderBuffer,
            )
        }.getOrNull()
    }

    override fun close() {
        runCatching {
            commandOutputStream.close()
        }
        runCatching {
            doorbellInputStream.close()
        }
        runCatching {
            recordInputStream.close()
        }
    }

    companion object {
        /**
         * Wraps raw host pipe fds returned by JNI in JVM streams/channels.
         *
         * This is test-only because the JDK does not expose a public
         * constructor for FileDescriptor(fd). Gradle opens java.io for this
         * opt-in test task so we can exercise the same fd protocol without an
         * emulator.
         */
        fun adopt(fds: IntArray?): HostOutboxPipeTransport {
            requireNotNull(fds) {
                "Host native outbox did not return pipe fds"
            }
            require(fds.size == PIPE_COUNT) {
                "Expected $PIPE_COUNT pipe fds, received ${fds.size}"
            }
            return HostOutboxPipeTransport(
                commandOutputStream = FileOutputStream(
                    fileDescriptorFromFd(fds[COMMAND_WRITE_FD_INDEX]),
                ),
                doorbellInputStream = FileInputStream(
                    fileDescriptorFromFd(fds[DOORBELL_READ_FD_INDEX]),
                ),
                recordInputStream = FileInputStream(
                    fileDescriptorFromFd(fds[RECORD_READ_FD_INDEX]),
                ),
            )
        }

        private fun fileDescriptorFromFd(fd: Int): FileDescriptor {
            return FileDescriptor().apply {
                fileDescriptorField.setInt(this, fd)
            }
        }

        private val fileDescriptorField: Field = FileDescriptor::class
            .java
            .getDeclaredField("fd")
            .apply {
                isAccessible = true
            }

        private const val PIPE_COUNT = 3
        private const val COMMAND_WRITE_FD_INDEX = 0
        private const val DOORBELL_READ_FD_INDEX = 1
        private const val RECORD_READ_FD_INDEX = 2
    }
}
