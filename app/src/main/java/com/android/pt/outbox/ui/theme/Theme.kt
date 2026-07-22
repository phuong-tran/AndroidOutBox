package com.android.pt.outbox.ui.theme

import android.os.Build
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.dynamicDarkColorScheme
import androidx.compose.material3.dynamicLightColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.platform.LocalContext

private val DarkColorScheme = darkColorScheme(
    primary = OutboxPrimary,
    onPrimary = OutboxOnPrimary,
    secondary = OutboxSuccess,
    tertiary = OutboxWarning,
    background = OutboxBackground,
    onBackground = OutboxTextStrong,
    surface = OutboxSurface,
    onSurface = OutboxTextStrong,
    surfaceVariant = OutboxSurfaceVariant,
    onSurfaceVariant = OutboxTextMuted,
    outline = OutboxOutline,
    inverseSurface = OutboxCodeSurface,
)

private val LightColorScheme = lightColorScheme(
    primary = OutboxPrimary,
    onPrimary = OutboxOnPrimary,
    secondary = OutboxSuccess,
    tertiary = OutboxWarning,
    background = OutboxBackground,
    onBackground = OutboxTextStrong,
    surface = OutboxSurface,
    onSurface = OutboxTextStrong,
    surfaceVariant = OutboxSurfaceVariant,
    onSurfaceVariant = OutboxTextMuted,
    outline = OutboxOutline,
    inverseSurface = OutboxCodeSurface,
)

@Composable
fun AndroidOutBoxTheme(
    darkTheme: Boolean = isSystemInDarkTheme(),
    // Dynamic color is available on Android 12+
    dynamicColor: Boolean = true,
    content: @Composable () -> Unit
) {
    val colorScheme = when {
        dynamicColor && Build.VERSION.SDK_INT >= Build.VERSION_CODES.S -> {
            val context = LocalContext.current
            if (darkTheme) dynamicDarkColorScheme(context) else dynamicLightColorScheme(context)
        }

        darkTheme -> DarkColorScheme
        else -> LightColorScheme
    }

    MaterialTheme(
        colorScheme = colorScheme,
        typography = Typography,
        content = content
    )
}
