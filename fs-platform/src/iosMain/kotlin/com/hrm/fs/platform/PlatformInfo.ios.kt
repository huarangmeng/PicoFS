package com.hrm.fs.platform

import platform.UIKit.UIDevice

class IOSPlatformInfo : PlatformInfo {
    override val name: String = UIDevice.currentDevice.systemName() + " " + UIDevice.currentDevice.systemVersion
}

actual fun getPlatformInfo(): PlatformInfo = IOSPlatformInfo()
