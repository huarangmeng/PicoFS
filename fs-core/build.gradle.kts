plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.androidMultiplatformLibrary)
    alias(libs.plugins.androidLint)
    alias(libs.plugins.kotlinxSerialization)
}

kotlin {
    jvmToolchain(21)

    androidLibrary {
        namespace = "com.hrm.fs.core"
        compileSdk = libs.versions.android.compileSdk.get().toInt()

        withHostTestBuilder {
        }

        withDeviceTestBuilder {
            sourceSetTreeName = "test"
        }.configure {
            instrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        }
    }

    listOf(
        iosArm64(),
        iosSimulatorArm64()
    ).forEach { iosTarget ->
        iosTarget.binaries.framework {
            baseName = "fsKit"
            isStatic = true
        }
    }

    jvm()

    sourceSets {
        commonMain {
            dependencies {
                implementation(projects.fsApi)
                implementation(projects.fsPlatform)
                implementation(libs.kotlinx.coroutines.core)
                implementation(libs.kotlinx.serialization.cbor)
            }
        }

        commonTest {
            dependencies {
                implementation(libs.kotlin.test)
                implementation(libs.kotlinx.coroutines.test)
            }
        }

        androidMain {
            dependencies {
            }
        }

        iosMain {
            dependencies {
            }
        }
        jvmMain {
            dependencies {
                implementation(libs.kotlinx.coroutinesSwing)
            }
        }
    }
}