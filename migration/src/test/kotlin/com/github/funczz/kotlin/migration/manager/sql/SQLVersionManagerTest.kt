package com.github.funczz.kotlin.migration.manager.sql

import com.github.funczz.kotlin.migration.SQLMigration
import com.github.funczz.kotlin.migration.manager.VersionManager
import com.github.funczz.kotlin.migration.model.Module
import com.github.funczz.kotlin.migration.util.sql.SQLConnectionExt
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager

@Suppress("NonAsciiCharacters")
class SQLVersionManagerTest : SQLConnectionExt {

    @Test
    fun `initialize を呼び出す`() {
        versionManager.initialize(context = context)
        assertEquals(true, connection.existsVersionsTable(), "expected: VERSION table exists = true")
    }

    @Test
    fun `currentVersionId の初期値`() {
        versionManager.initialize(context = context)
        val actual = versionManager.getCurrentVersionId(context = context)
        assertEquals("", actual, "初期値はブランク")
    }

    @Test
    fun `currentVersionId の更新と取得`() {
        val expected = "migration_1.0.0"
        versionManager.initialize(context = context)
        versionManager.setCurrentVersionId(versionId = expected, context = context)
        val actual = versionManager.getCurrentVersionId(context = context)
        assertEquals(expected, actual)
    }

    private lateinit var versionManager: VersionManager

    private lateinit var connection: Connection

    private lateinit var context: Map<String, Any>

    @BeforeEach
    fun beforeEach() {
        connection = DriverManager.getConnection("jdbc:hsqldb:mem:test_db", "sa", "sa")
        connection.autoCommit = false
        context = mapOf(Pair(SQLMigration.CONNECTION, connection))
        versionManager = SQLVersionManager(module = Module(moduleId = "module1"))
    }

    @AfterEach
    fun afterEach() {
        connection.use { it.createStatement().executeUpdate("SHUTDOWN") }
    }
}