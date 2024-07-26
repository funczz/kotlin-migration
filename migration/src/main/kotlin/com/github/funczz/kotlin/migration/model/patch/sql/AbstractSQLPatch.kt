package com.github.funczz.kotlin.migration.model.patch.sql

import com.github.funczz.kotlin.migration.SQLMigration
import com.github.funczz.kotlin.migration.model.patch.Patch
import com.github.funczz.kotlin.migration.util.sql.SQLConnectionExt
import java.sql.Connection
import java.util.*


@Suppress("Unused", "MemberVisibilityCanBePrivate")
abstract class AbstractSQLPatch : Patch<String>, SQLConnectionExt {

    override fun migrate(
        moduleId: String,
        versionId: String,
        tags: Array<out String>,
        context: Map<String, Any>
    ) {
        update(sql = up(), tags = tags, context = context)
    }

    override fun rollback(
        moduleId: String,
        versionId: String,
        tags: Array<out String>,
        context: Map<String, Any>
    ) {
        update(sql = down(), tags = tags, context = context)
    }

    private fun update(sql: String, tags: Array<out String>, context: Map<String, Any>) {
        if (!contains(tags = tags)) return
        val connection = context[SQLMigration.CONNECTION] as Connection
        connection.update(sql)
    }


    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        other as SQLPatch
        return getTag() == other.getTag() && up() == other.up() && down() == other.down()
    }

    override fun hashCode(): Int {
        var result = 17
        result = 31 * result + Objects.hashCode(getTag())
        result = 31 * result + Objects.hashCode(up())
        result = 31 * result + Objects.hashCode(down())
        return result
    }
}