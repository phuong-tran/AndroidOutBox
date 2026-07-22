package io.github.phuongtran.androidoutbox

/**
 * Test-only JNI entrypoint for loading the host shared library built by Gradle.
 *
 * This mirrors production's thin JNI boundary but avoids Android parcel fd
 * wrappers, allowing JVM tests to exercise the native pipe protocol on the
 * developer/CI host OS when the opt-in Gradle flag is set.
 */
internal object HostOutboxNativeBridge {
    fun load(): Boolean {
        val libraryPath = System.getProperty(HOST_JNI_LIBRARY_PROPERTY)
        if (libraryPath.isNullOrEmpty()) {
            return false
        }
        System.load(libraryPath)
        return true
    }

    @JvmStatic
    external fun nativeOpenPipes(): IntArray?

    private const val HOST_JNI_LIBRARY_PROPERTY = "androidoutbox.hostJniLibrary"
}
