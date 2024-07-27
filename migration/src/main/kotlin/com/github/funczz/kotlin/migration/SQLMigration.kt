package com.github.funczz.kotlin.migration

import com.github.funczz.kotlin.migration.manager.VersionManager
import com.github.funczz.kotlin.migration.manager.sql.SQLVersionManager
import com.github.funczz.kotlin.migration.model.Module
import com.github.funczz.kotlin.migration.util.sql.SQLConnectionExt
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.locks.ReentrantLock
import javax.sql.DataSource
import kotlin.concurrent.withLock

@Suppress("Unused", "MemberVisibilityCanBePrivate")
open class SQLMigration(

    private var module: Module,

    private var connector: () -> Connection,

    private var transactionIsolation: Int? = null,

    private var versionManager: VersionManager = SQLVersionManager(module = module)

) : Migration, SQLConnectionExt {

    constructor(
        module: Module,
        dataSource: DataSource,
        transactionIsolation: Int? = null,
        versionManager: VersionManager = SQLVersionManager(module = module)
    ) : this(
        module = module,
        connector = { dataSource.connection },
        transactionIsolation = transactionIsolation,
        versionManager = versionManager,
    )

    constructor(
        module: Module,
        url: String,
        username: String,
        password: String,
        transactionIsolation: Int? = null,
        versionManager: VersionManager = SQLVersionManager(module = module)
    ) : this(
        module = module,
        connector = { DriverManager.getConnection(url, username, password) },
        transactionIsolation = transactionIsolation,
        versionManager = versionManager,
    )

    private val lock = ReentrantLock()

    private fun <R> commit(function: (Connection) -> R): R = lock.withLock {
        val connection = connector()
        connection.commit(transactionIsolation = transactionIsolation, function = function)
    }

    override fun initialize() = commit { connection ->
        versionManager.initialize(context = newContext(connection = connection))
    }

    override fun migrate() {
        migrate(tags = arrayOf())
    }

    override fun migrate(tags: Array<out String>) {
        val versions = module.getVersions()
        if (versions.isEmpty()) return //Version が未登録なら何もしない
        migrate(versionId = versions.last().getVersionId(), tags = tags)
    }

    override fun migrate(versionId: String) {
        migrate(versionId = versionId, tags = arrayOf())
    }

    override fun migrate(versionId: String, tags: Array<out String>) = commit { connection ->
        val context = newContext(connection = connection)
        val versions = module.getVersions()

        if (versions.isEmpty()) return@commit //Version が未登録なら何もしない
        require(versions.any { it.getVersionId() == versionId }) //versionId と同値の Version がなければ例外エラー

        val currentVersionId = versionManager.getCurrentVersionId(context = context)
        require(
            currentVersionId.isBlank() || versions.any { it.getVersionId() == currentVersionId }
        ) //currentVersionId と同値の Version がなければ例外エラー

        if (versions.last().getVersionId() == currentVersionId) {
            return@commit
        } //Version が全て適用済みなら何もしない

        val versionIdIndex = module.getVersions().indexOfFirst { it.getVersionId() == versionId }
        val currentVersionIdIndex =
            module.getVersions().indexOfFirst { it.getVersionId() == currentVersionId }
        if (versionIdIndex <= currentVersionIdIndex) return@commit //versionId と同値の Version が適用済みなら何もしない

        val startVersionIndex = when {
            currentVersionId.isBlank() -> 0 //currentVersionId がブランクなら全て未適用なので開始インデックスを0にセットする
            else -> currentVersionIdIndex + 1 //開始インデックスを currentVersionId の次にセットする
        }
        for ((index, version) in versions.withIndex()) {
            if (index < startVersionIndex) continue //開始インデックスまで適用をスキップ
            for (patch in version.getPatches()) {
                patch.migrate(
                    moduleId = module.getModuleId(),
                    versionId = version.getVersionId(),
                    tags = tags,
                    context = context
                )
            }
            versionManager.setCurrentVersionId(
                versionId = version.getVersionId(),
                context = context
            )
            connection.commit() //バージョン毎に結果をコミットする
            if (version.getVersionId() == versionId) break //適用した Version が versionID と同値なら適用を終える
        }
    }

    override fun rollback() {
        rollback(tags = arrayOf())
    }

    override fun rollback(tags: Array<out String>) = commit { connection ->
        val context = newContext(connection = connection)
        val versions = module.getVersions()

        if (versions.isEmpty()) return@commit //Version が未登録なら何もしない

        val currentVersionId = versionManager.getCurrentVersionId(
            context = context
        )
        if (currentVersionId.isBlank()) return@commit //currentVersionId がブランクなら全て未適用なので何もしない
        require(versions.any { it.getVersionId() == currentVersionId }) //currentVersionId と同値の Version がなければ例外エラー

        val versionIndex = module.getVersions().indexOfLast {
            it.getVersionId() == currentVersionId
        }
        val version = versions[versionIndex]
        for (patch in version.getPatches()) {
            patch.rollback(
                moduleId = module.getModuleId(),
                versionId = version.getVersionId(),
                tags = tags,
                context = context
            )
        }
        val newCurrentVersionId = when {
            versionIndex == 0 -> ""
            else -> versions[versionIndex - 1].getVersionId()
        }
        versionManager.setCurrentVersionId(
            versionId = newCurrentVersionId,
            context = context
        )
    }

    override fun getCurrentVersionId(): String = commit { connection ->
        versionManager.getCurrentVersionId(context = newContext(connection = connection))
    }

    private fun newContext(connection: Connection): Map<String, Any> {
        val context = mutableMapOf<String, Any>()
        context[CONNECTION] = connection
        return context
    }

    companion object {
        const val CONNECTION: String = "SQLMigration.context.connection"
    }
}