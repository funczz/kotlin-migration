package com.github.funczz.kotlin.migration.manager

interface VersionManager {

    fun initialize(context: Map<String, Any>)

    fun setCurrentVersionId(versionId: String, context: Map<String, Any>)

    fun getCurrentVersionId(context: Map<String, Any>): String

}