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
        try {
            versionManager.initialize(context = newContext(connection = connection))
        } catch (th: Throwable) {
            throw IllegalVersionException("Could not initialize version manager.", cause = th)
        }
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

        if (!(versions.any { it.getVersionId() == versionId })) {
            throw IllegalVersionException(
                "Module does not contain requested version id: requested version id=`$versionId`"
            )
        } //versionId と同値の Version がなければ例外エラー

        val currentVersionId = getCurrentVersionId(context = context)

        if (!(currentVersionId.isBlank() || versions.any { it.getVersionId() == currentVersionId })) {
            throw IllegalVersionException(
                "Module does not contain current version id: current version id=`$currentVersionId`"
            )
        } //currentVersionId と同値の Version がなければ例外エラー

        if (versions.last().getVersionId() == currentVersionId) {
            return@commit
        } //Version が全て適用済みなら何もしない

        val versionIdIndex = module.getVersions().indexOfFirst {
            it.getVersionId() == versionId
        }
        val currentVersionIdIndex = module.getVersions().indexOfFirst {
            it.getVersionId() == currentVersionId
        }
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
            setCurrentVersionId(versionId = version.getVersionId(), context = context)
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

        val currentVersionId = getCurrentVersionId(context = context)

        if (currentVersionId.isBlank()) return@commit //currentVersionId がブランクなら全て未適用なので何もしない

        if (!(versions.any { it.getVersionId() == currentVersionId })) {
            throw IllegalVersionException(
                "Module does not contain current version id: current version id=`$currentVersionId`"
            )
        } //currentVersionId と同値の Version がなければ例外エラー

        val versionIndex = module.getVersions().indexOfLast {
            it.getVersionId() == currentVersionId
        }
        val version = versions[versionIndex]
        for (patch in version.getPatches().reversed()) { //migrateと逆順でrollbackを適用する
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
        setCurrentVersionId(versionId = newCurrentVersionId, context = context)
    }

    override fun getCurrentVersionId(): String = commit { connection ->
        getCurrentVersionId(context = newContext(connection = connection))
    }

    private fun getCurrentVersionId(context: Map<String, Any>): String {
        return try {
            versionManager.getCurrentVersionId(context = context)
        } catch (th: Throwable) {
            throw IllegalVersionException(
                "Could not retrieve current version id.",
                cause = th
            )
        }
    }

    private fun setCurrentVersionId(versionId: String, context: Map<String, Any>) {
        try {
            versionManager.setCurrentVersionId(versionId = versionId, context = context)
        } catch (th: Throwable) {
            throw IllegalVersionException(
                "Could not save current version id: current version id=`$versionId`",
                cause = th
            )
        }
    }

    private fun newContext(connection: Connection): Map<String, Any> {
        val context = mutableMapOf<String, Any>()
        context[CONNECTION] = connection
        return context
    }

    companion object {
        const val CONNECTION: String = "SQLMigration.context.connection"
    }

    class IllegalVersionException(message: String, cause: Throwable? = null) : Exception(message, cause) {
        companion object {
            private const val serialVersionUID: Long = 4504669497268338744L
        }
    }
}