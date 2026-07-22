package io.github.phuongtran.androidoutbox

import android.os.ParcelFileDescriptor
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/**
 * Owns the Kotlin side of native-created pipe file descriptors.
 *
 * This layer is intentionally domain-blind. It writes and reads length-prefixed
 * fd frames only; command, response, and doorbell meaning are decoded one layer
 * above.
 *
 * The native library returns already-dup'd fds for Kotlin. This class adopts
 * those fds exactly once and closes them on [close]. Channels are used over the
 * AutoClose streams so command envelopes can be written from [ByteBuffer]
 * duplicates without copying into temporary byte arrays.
 */
internal class OutboxNativePipeTransport private constructor(
    private val commandParcelFileDescriptor: ParcelFileDescriptor,
    private val doorbellParcelFileDescriptor: ParcelFileDescriptor,
    private val recordParcelFileDescriptor: ParcelFileDescriptor,
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

    /**
     * Writes a complete command frame to the native control pipe.
     *
     * The command pipe is a single FIFO lane, so concurrent callers synchronize
     * here before the control client waits for the matching response sequence.
     */
    override fun writeCommandEnvelope(envelope: ByteBuffer): Boolean {
        return runCatching {
            synchronized(commandOutputChannel) {
                val writeBuffer = envelope.duplicate()
                OutboxFdFrameCodec.writeFrame(
                    outputChannel = commandOutputChannel,
                    payload = writeBuffer,
                    headerBuffer = commandHeaderBuffer,
                )
            }
            true
        }.getOrDefault(false)
    }

    /**
     * Reads native wakeups such as HANDSHAKE or DATA_AVAILABLE.
     */
    override fun readDoorbellFrame(): ByteArray? {
        return runCatching {
            OutboxFdFrameCodec.readFrame(
                inputChannel = doorbellInputChannel,
                headerBuffer = doorbellHeaderBuffer,
            )
        }.getOrNull()
    }

    /**
     * Reads responses for commands sent through [writeCommandEnvelope].
     */
    override fun readControlResponseFrame(): ByteArray? {
        return runCatching {
            OutboxFdFrameCodec.readFrame(
                inputChannel = recordInputChannel,
                headerBuffer = recordHeaderBuffer,
            )
        }.getOrNull()
    }

    /**
     * Closes every Kotlin-owned fd handle. Each call is best-effort because
     * native shutdown can race with app lifecycle teardown.
     */
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
        runCatching {
            commandParcelFileDescriptor.close()
        }
        runCatching {
            doorbellParcelFileDescriptor.close()
        }
        runCatching {
            recordParcelFileDescriptor.close()
        }
    }

    companion object {
        /**
         * Adopts native-returned fd integers into Android [ParcelFileDescriptor]
         * owners. Returns null if the JNI bridge and Kotlin disagree on pipe
         * layout, which lets callers fall back to no-op logging.
         */
        fun adopt(fds: IntArray): OutboxNativePipeTransport? {
            if (fds.size != PIPE_COUNT) {
                return null
            }
            val adoptedFds = ArrayList<ParcelFileDescriptor>(PIPE_COUNT)
            return runCatching {
                val commandPfd = ParcelFileDescriptor
                    .adoptFd(fds[COMMAND_WRITE_FD_INDEX])
                    .also(adoptedFds::add)
                val doorbellPfd = ParcelFileDescriptor
                    .adoptFd(fds[DOORBELL_READ_FD_INDEX])
                    .also(adoptedFds::add)
                val recordPfd = ParcelFileDescriptor
                    .adoptFd(fds[RECORD_READ_FD_INDEX])
                    .also(adoptedFds::add)
                OutboxNativePipeTransport(
                    commandParcelFileDescriptor = commandPfd,
                    doorbellParcelFileDescriptor = doorbellPfd,
                    recordParcelFileDescriptor = recordPfd,
                    commandOutputStream = ParcelFileDescriptor.AutoCloseOutputStream(commandPfd),
                    doorbellInputStream = ParcelFileDescriptor.AutoCloseInputStream(doorbellPfd),
                    recordInputStream = ParcelFileDescriptor.AutoCloseInputStream(recordPfd),
                )
            }.onFailure {
                adoptedFds.forEach { fd ->
                    runCatching {
                        fd.close()
                    }
                }
            }.getOrNull()
        }

        private const val PIPE_COUNT = 3
        private const val COMMAND_WRITE_FD_INDEX = 0
        private const val DOORBELL_READ_FD_INDEX = 1
        private const val RECORD_READ_FD_INDEX = 2
    }
}
