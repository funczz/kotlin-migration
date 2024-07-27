package com.github.funczz.kotlin.migration.model.patch.sql

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.nio.charset.Charset
import java.util.*

@Suppress("Unused", "MemberVisibilityCanBePrivate")
open class UpSQLPatch(

    private val tag: String,

    ) : AbstractSQLPatch() {

    private lateinit var _up: String

    constructor(up: String, tag: String = "") : this(tag = tag) {
        this._up = up
    }

    constructor(up: InputStream, tag: String = "") : this(tag = tag) {
        this._up = up.asString() ?: ""
    }

    constructor(
        classLoader: ClassLoader,
        up: String,
        tag: String = ""
    ) : this(tag = tag) {
        this._up = classLoader.getResourceAsStream(up)?.asString() ?: ""
    }

    override fun getTag(): String {
        return tag
    }

    override fun up(): String {
        return _up
    }

    override fun down(): String {
        throw IrreversibleMigrationException(
            message = "%s(tag=%s,up=`%s`)".format(
                this::class.simpleName,
                tag,
                up()
            )
        )
    }

    private fun InputStream.asString(): String? {
        return this.use {
            val out = ByteArrayOutputStream()
            val buf = ByteArray(1024 * 8)
            var length: Int
            while ((this.read(buf).also { length = it }) != -1) {
                out.write(buf, 0, length)
            }
            out.toString(Charset.defaultCharset().toString())
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        other as UpSQLPatch
        return getTag() == other.getTag() && up() == other.up()
    }

    override fun hashCode(): Int {
        var result = 17
        result = 31 * result + Objects.hashCode(getTag())
        result = 31 * result + Objects.hashCode(up())
        return result
    }

    override fun toString(): String {
        return "%s(tag=%s)".format(
            this::class.simpleName,
            getTag(),
        )
    }

    class IrreversibleMigrationException(override val message: String?) : Exception(message)
}

