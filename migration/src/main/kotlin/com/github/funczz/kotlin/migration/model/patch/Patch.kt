package com.github.funczz.kotlin.migration.model.patch

interface Patch<T : Any> {

    fun getTag(): String

    fun up(): T

    fun down(): T

    fun migrate(
        moduleId: String,
        versionId: String,
        tags: Array<out String>,
        context: Map<String, Any>
    )

    fun rollback(
        moduleId: String,
        versionId: String,
        tags: Array<out String>,
        context: Map<String, Any>
    )

    fun contains(tags: Array<out String>): Boolean {
        if (getTag().isBlank()) return true
        return tags.contains(getTag())
    }

}
