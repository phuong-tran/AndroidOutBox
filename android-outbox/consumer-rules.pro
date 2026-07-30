# Keep JNI entry points stable when an app minifies dependencies.
# AndroidOutBox's native bridge resolves NativeAndroidOutbox.nativeOpenPipes by
# class and method name through the generated JNI symbol.
-keepclasseswithmembernames,includedescriptorclasses class io.github.phuongtran.androidoutbox.** {
    native <methods>;
}
