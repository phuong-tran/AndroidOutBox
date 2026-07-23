cmake_minimum_required(VERSION 3.22.1)
project("android_outbox")

add_library(android_outbox SHARED
        outbox_control.c
        outbox_core.c
        outbox_frame.c
        outbox_queue.c
        outbox_spool.c
        outbox_writer.c
        outbox_jni.cpp)

target_compile_options(android_outbox PRIVATE
        -fPIC
        -D_FORTIFY_SOURCE=2
        -Wall
        -Wextra
        -Werror)

set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_library(log-lib log)

target_link_libraries(android_outbox
        android
        ${log-lib}
        -Wl,--no-as-needed
        -Wl,--no-rosegment
        -Wl,--hash-style=both)
