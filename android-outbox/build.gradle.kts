plugins {
    alias(libs.plugins.android.library)
    `maven-publish`
}

android {
    namespace = "io.github.phuongtran.androidoutbox"
    compileSdk {
        version = release(36) {
            minorApiLevel = 1
        }
    }

    defaultConfig {
        minSdk = 24
        consumerProguardFiles("consumer-rules.pro")
    }

    ndkVersion = libs.versions.ndk.get()

    externalNativeBuild {
        cmake {
            path = file("src/main/cpp/CMakeLists.txt")
            version = "3.22.1"
        }
    }

    publishing {
        singleVariant("release") {
            withSourcesJar()
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
}

dependencies {
    api(libs.kotlinx.coroutines.core)

    testImplementation(libs.junit)
}

publishing {
    publications {
        register<MavenPublication>("release") {
            groupId = providers.gradleProperty("GROUP").get()
            artifactId = "android-outbox"
            version = providers.gradleProperty("VERSION_NAME").get()

            pom {
                name.set("AndroidOutBox")
                description.set("A small Android native-backed outbox for app-owned records.")
                url.set("https://github.com/phuong-tran/AndroidOutBox")

                licenses {
                    license {
                        name.set("Apache License 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }

                developers {
                    developer {
                        id.set("phuong-tran")
                        name.set("Phuong Tran")
                    }
                }

                scm {
                    connection.set("scm:git:git://github.com/phuong-tran/AndroidOutBox.git")
                    developerConnection.set("scm:git:ssh://github.com/phuong-tran/AndroidOutBox.git")
                    url.set("https://github.com/phuong-tran/AndroidOutBox")
                }
            }
        }
    }

    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/phuong-tran/AndroidOutBox")
            credentials {
                username = providers.gradleProperty("gpr.user")
                    .orElse(providers.environmentVariable("GITHUB_ACTOR"))
                    .orNull
                password = providers.gradleProperty("gpr.key")
                    .orElse(providers.environmentVariable("GITHUB_TOKEN"))
                    .orNull
            }
        }
    }
}

afterEvaluate {
    publishing {
        publications.named<MavenPublication>("release") {
            from(components["release"])
        }
    }
}

// Host-native diagnostics are manual-only and live outside Android .so packaging.
apply(from = "native-host-tests.gradle")
