# This is a configuration file for ProGuard.
# http://proguard.sourceforge.net/index.html#manual/usage.html
# The following are the most useful options for configuring ProGuard:

# Optimization is turned off by default. Dex does not like code run
# through the ProGuard optimize and preverify steps (and performs some
# of these optimizations on its own).
-dontoptimize

# Note that if you want to enable optimization, you cannot just
# include optimization flags in this configuration file; instead you
# will need to explicitly invoke ProGuard's maximum optimization
# settings, i.e., -O9.

-keepattributes *Annotation*
-keep public class com.google.android.material.R
-keep public class androidx.** { public *; }
-keep interface androidx.** { *; }

# The -optimizationpasses option with a higher number in the
# configuration file tells ProGuard to do the same optimization
# up to 5 times.

# If you want to have local variable names preserved, use the
# variable name table. Otherwise local variable type information
# will be preserved, which is enough to restore the stack map table.

# Keep classes that are referenced on the manifest
-keep public class * extends android.app.Activity
-keep public class * extends android.app.Service
-keep public class * extends android.content.BroadcastReceiver
-keep public class * extends android.content.ContentProvider
-keep public class * extends android.view.View {
    public <init>(android.content.Context);
    public <init>(android.content.Context, android.util.AttributeSet);
}

# Preserve native methods
-keepclasseswithmembernames class * {
    native <methods>;
}

# Preserve custom application classes that extend framework classes
-keep public class * extends android.app.Application
