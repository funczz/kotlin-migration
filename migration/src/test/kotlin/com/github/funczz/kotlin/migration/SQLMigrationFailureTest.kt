package com.github.funczz.kotlin.migration

import com.github.funczz.kotlin.migration.model.Module
import com.github.funczz.kotlin.migration.model.Version
import com.github.funczz.kotlin.migration.model.patch.sql.SQLPatch
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLSyntaxErrorException

@Suppress("NonAsciiCharacters")
class SQLMigrationFailureTest {

    @Test
    fun `migrate の呼び出しでエラーが発生する`() {
        val module = Module(
            moduleId = "module1",
            Version(
                versionId = "version1",
                SQLPatch(up = "FOO", down = "BAR")
            )
        )
        migration = SQLMigration(connector = connector, module = module)
        migration.initialize()
        assertThrows<SQLSyntaxErrorException> {
            migration.migrate()
        }
        assertEquals("", migration.getCurrentVersionId())
    }

    @Test
    fun `rollback の呼び出しでエラーが発生する`() {
        val module = Module(
            moduleId = "module1",
            Version(
                versionId = "version1",
                SQLPatch(
                    up = "CREATE TABLE PATCH1 (ID INT PRIMARY KEY NOT NULL, NAME VARCHAR(100) NOT NULL)",
                    down = "BAR"
                )
            )
        )
        migration = SQLMigration(connector = connector, module = module)
        migration.initialize()
        migration.migrate()
        assertEquals(module.getVersions().first().getVersionId(), migration.getCurrentVersionId())
        assertThrows<SQLSyntaxErrorException> {
            migration.rollback()
        }
        assertEquals(module.getVersions().first().getVersionId(), migration.getCurrentVersionId())
    }

    private lateinit var migration: Migration

    private lateinit var connector: () -> Connection

    @BeforeEach
    fun beforeEach() {
        connector = {
            DriverManager.getConnection("jdbc:hsqldb:mem:test_db", "sa", "sa").also {
                it.autoCommit = false
            }
        }
    }

    @AfterEach
    fun afterEach() {
        connector().use { it.createStatement().executeUpdate("SHUTDOWN") }
    }
}