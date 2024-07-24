package com.github.funczz.kotlin.migration

interface Migration {

    fun initialize()

    fun migrate()

    fun migrate(tags: Array<out String>)

    fun migrate(versionId: String)

    fun migrate(versionId: String, tags: Array<out String>)

    fun rollback()

    fun rollback(tags: Array<out String>)

    fun getCurrentVersionId(): String

}