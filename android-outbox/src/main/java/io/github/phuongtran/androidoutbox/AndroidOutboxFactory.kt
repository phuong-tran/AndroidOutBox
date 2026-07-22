package io.github.phuongtran.androidoutbox

object AndroidOutboxFactory {
    /**
     * Creates an outbox that fails open when the native library cannot be loaded.
     */
    fun create(): AndroidOutbox {
        return if (NativeAndroidOutbox.isAvailable) {
            NativeAndroidOutbox
        } else {
            NoOpAndroidOutbox
        }
    }
}
