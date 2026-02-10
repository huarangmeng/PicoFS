plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.androidMultiplatformLibrary)
    alias(libs.plugins.androidLint)
}

kotlin {
    jvmToolchain(21)

    androidLibrary {
        namespace = "com.hrm.fs"
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
            }
        }

        commonTest {
            dependencies {
                implementation(libs.kotlin.test)
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
        jvmMain.dependencies {
        }
    }
}