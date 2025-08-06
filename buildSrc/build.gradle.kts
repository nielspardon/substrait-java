plugins {
  `kotlin-dsl`
  id("com.diffplug.spotless") version "7.2.1"
}

repositories { gradlePluginPortal() }

spotless {
  kotlinGradle { ktfmt().googleStyle() }
}
