package com.github.funczz.kotlin.migration.model

import java.util.*

@Suppress("Unused", "MemberVisibilityCanBePrivate")
class Module {

    constructor()

    constructor(moduleId: String) : this() {
        _moduleId = moduleId
    }

    constructor(moduleId: String, versions: List<Version>) : this() {
        _moduleId = moduleId
        _versions.addAll(versions)
    }

    constructor(moduleId: String, vararg versions: Version) : this() {
        _moduleId = moduleId
        _versions.addAll(versions)
    }

    private var _moduleId: String = ""

    private val _versions = mutableListOf<Version>()

    fun getModuleId(): String = _moduleId

    fun getVersions(): List<Version> = _versions.toList()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        other as Module
        return getModuleId() == other.getModuleId()
    }

    override fun hashCode(): Int {
        var result = 17
        result = 31 * result + Objects.hashCode(_moduleId)
        return result
    }

    override fun toString(): String {
        return "%s(moduleId=%s, versions=%s)".format(
            this::class.simpleName,
            _moduleId,
            _versions.joinToString(",") { it.toString() },
        )
    }
}