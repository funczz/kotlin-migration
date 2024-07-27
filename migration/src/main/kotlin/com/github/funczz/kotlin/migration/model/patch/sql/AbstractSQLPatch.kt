package com.github.funczz.kotlin.migration.model.patch.sql

import com.github.funczz.kotlin.migration.SQLMigration
import com.github.funczz.kotlin.migration.model.patch.Patch
import com.github.funczz.kotlin.migration.util.sql.SQLConnectionExt
import java.sql.Connection


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

}