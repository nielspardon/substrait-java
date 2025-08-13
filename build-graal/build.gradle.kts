plugins {
  `java-gradle-plugin`
  `groovy`
  alias(libs.plugins.spotless)
}

repositories { mavenCentral() }

gradlePlugin {
  plugins {
    create("graal") {
      id = "io.substrait.graal"
      displayName = "Substrait Graal Plugin"
      implementationClass = "com.palantir.gradle.graal.GradleGraalPlugin"
    }
  }
}

dependencies {
  implementation(localGroovy())
  implementation(gradleApi())

  testImplementation(gradleTestKit())
  testImplementation(platform("org.spockframework:spock-bom:2.3-groovy-3.0"))
  testImplementation("org.spockframework:spock-core")
  testImplementation("com.netflix.nebula:nebula-test:11.1.4")
  testImplementation("com.squareup.okhttp3:mockwebserver:5.1.0")
}

spotless {
  kotlinGradle { ktfmt().googleStyle() }
  java {
    target("src/*/java/**/*.java")
    googleJavaFormat()
    removeUnusedImports()
    trimTrailingWhitespace()
    removeWildcardImports()
  }
}
