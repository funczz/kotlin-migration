package com.github.funczz.kotlin.migration.model

import com.github.funczz.kotlin.migration.model.patch.Patch
import java.util.*

@Suppress("Unused", "MemberVisibilityCanBePrivate")
class Version {

    constructor()

    constructor(versionId: String) : this() {
        _versionId = versionId
    }

    constructor(versionId: String, patches: List<Patch<*>>) : this() {
        _versionId = versionId
        _patches.addAll(patches)
    }

    constructor(versionId: String, vararg patches: Patch<*>) : this() {
        _versionId = versionId
        _patches.addAll(patches)
    }

    private var _versionId: String = ""

    private val _patches = mutableListOf<Patch<*>>()

    fun getVersionId(): String = _versionId

    fun getPatches(): List<Patch<*>> = _patches.toList()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        other as Version
        return getVersionId() == other.getVersionId()
    }

    override fun hashCode(): Int {
        var result = 17
        result = 31 * result + Objects.hashCode(_versionId)
        return result
    }

    override fun toString(): String {
        return "%s(versionId=%s, patches=%s)".format(
            this::class.simpleName,
            _versionId,
            _patches.joinToString(",") { it.toString() },
        )
    }
}