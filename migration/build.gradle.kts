/**
 * plugins
 */
plugins {
    id("nebula.maven-publish") version "18.4.0"
}

/**
 * buildscript
 */
buildscript {
    dependencies {
    }
}

/**
 * dependencies
 */
dependencies {
    testImplementation("com.h2database:h2:2.2.224")
    testImplementation("org.hsqldb:hsqldb:2.5.2")
}

/**
 * plugin: nebula.maven-publish
 */
publishing {
    publications {
        group = "com.github.funczz"
    }

    repositories {
        maven {
            url = uri(
                PublishMavenRepository.url(
                    version = version.toString(),
                    baseUrl = "$buildDir/mvn-repos"
                )
            )
        }
    }
}
