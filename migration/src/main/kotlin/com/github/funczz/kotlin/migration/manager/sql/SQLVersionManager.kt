package com.github.funczz.kotlin.migration.manager.sql

import com.github.funczz.kotlin.migration.SQLMigration
import com.github.funczz.kotlin.migration.manager.VersionManager
import com.github.funczz.kotlin.migration.model.Module
import com.github.funczz.kotlin.migration.util.sql.SQLConnectionExt
import java.sql.Connection
import java.util.*

open class SQLVersionManager(

    private val module: Module,

    ) : VersionManager, SQLConnectionExt {

    override fun initialize(context: Map<String, Any>) {
        val connection = getConnection(context = context)
        if (connection.existsVersionsTable()) return
        connection.update(
            "CREATE TABLE VERSIONS (MODULE_ID VARCHAR(100) PRIMARY KEY NOT NULL, VERSION_ID VARCHAR(100) NOT NULL)"
        )
    }

    override fun setCurrentVersionId(versionId: String, context: Map<String, Any>) {
        val connection = getConnection(context = context)
        if (connection.update(
                "UPDATE VERSIONS SET VERSION_ID = ? WHERE MODULE_ID = ?",
                versionId,
                module.getModuleId(),
            ) == 0
        ) {
            connection.update(
                "INSERT INTO VERSIONS (MODULE_ID, VERSION_ID) VALUES (?, ?)",
                module.getModuleId(),
                versionId
            )
        }
    }

    override fun getCurrentVersionId(context: Map<String, Any>): String {
        val connection = getConnection(context = context)
        val optional: Optional<String> = connection.select(
            "SELECT VERSION_ID FROM VERSIONS WHERE MODULE_ID = ?",
            module.getModuleId()
        ) {
            it.getString(1)
        }
        return when (optional.isPresent) {
            true -> optional.get()
            else -> ""
        }
    }

    private fun getConnection(context: Map<String, Any>): Connection {
        return context[SQLMigration.CONNECTION] as Connection
    }

}