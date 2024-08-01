package com.github.funczz.kotlin.migration

import com.github.funczz.kotlin.migration.model.Module
import com.github.funczz.kotlin.migration.model.Version
import com.github.funczz.kotlin.migration.model.patch.sql.SQLPatch
import com.github.funczz.kotlin.migration.util.sql.SQLConnectionExt
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager

@Suppress("NonAsciiCharacters")
class SQLMigrationTest : SQLConnectionExt {

    @Test
    fun `versionId, tag を指定しないで migrate を呼び出す`() {
        val expected = listOf("PATCH1", "PATCH2", "", "PATCH4")
        migration.initialize()
        migration.migrate()
        val actual = mutableListOf<String>()
        for (tableName in listOf("PATCH1", "PATCH2", "PATCH3", "PATCH4")) {
            val result = when (connector().existsTable(name = tableName)) {
                true -> tableName
                else -> ""
            }
            actual.add(result)
        }
        assertEquals(expected, actual)
        assertEquals(version2.getVersionId(), migration.getCurrentVersionId())
    }

    @Test
    fun `tag を指定して migrate を呼び出す`() {
        val expected = listOf("PATCH1", "PATCH2", "PATCH3", "PATCH4")
        migration.initialize()
        migration.migrate(tags = arrayOf("tag1"))
        val actual = mutableListOf<String>()
        for (tableName in listOf("PATCH1", "PATCH2", "PATCH3", "PATCH4")) {
            val result = when (connector().existsTable(name = tableName)) {
                true -> tableName
                else -> ""
            }
            actual.add(result)
        }
        assertEquals(expected, actual)
        assertEquals(version2.getVersionId(), migration.getCurrentVersionId())
    }

    @Test
    fun `versionId を指定して migrate を呼び出す`() {
        val expected = listOf("PATCH1", "PATCH2", "", "")
        migration.initialize()
        migration.migrate(versionId = version1.getVersionId())
        val actual = mutableListOf<String>()
        for (tableName in listOf("PATCH1", "PATCH2", "PATCH3", "PATCH4")) {
            val result = when (connector().existsTable(name = tableName)) {
                true -> tableName
                else -> ""
            }
            actual.add(result)
        }
        assertEquals(expected, actual)
        assertEquals(version1.getVersionId(), migration.getCurrentVersionId())
    }

    @Test
    fun `migrate を複数回呼び出す`() {
        val expected = listOf("PATCH1", "PATCH2", "", "PATCH4")
        migration.initialize()
        migration.migrate()
        migration.migrate()
        val actual = mutableListOf<String>()
        for (tableName in listOf("PATCH1", "PATCH2", "PATCH3", "PATCH4")) {
            val result = when (connector().existsTable(name = tableName)) {
                true -> tableName
                else -> ""
            }
            actual.add(result)
        }
        assertEquals(expected, actual)
        assertEquals(version2.getVersionId(), migration.getCurrentVersionId())
    }

    @Test
    fun `適用済みの versionId を指定して migrate を呼び出す`() {
        val expected = listOf("PATCH1", "PATCH2", "", "PATCH4")
        val migration2 = SQLMigration(connector = connector, module = module2)
        migration2.initialize()
        migration2.migrate(versionId = version2.getVersionId())
        migration2.migrate(versionId = version1.getVersionId())
        val actual = mutableListOf<String>()
        for (tableName in listOf("PATCH1", "PATCH2", "PATCH3", "PATCH4")) {
            val result = when (connector().existsTable(name = tableName)) {
                true -> tableName
                else -> ""
            }
            actual.add(result)
        }
        assertEquals(expected, actual)
        assertEquals(version2.getVersionId(), migration.getCurrentVersionId())
    }

    @Test
    fun `rollback を呼び出す`() {
        migration.initialize()
        migration.migrate()
        assertEquals(version2.getVersionId(), migration.getCurrentVersionId(), "migrate呼び出し後")
        migration.rollback()
        var expected = listOf("PATCH1", "PATCH2", "", "")
        var actual = mutableListOf<String>()
        for (tableName in listOf("PATCH1", "PATCH2", "PATCH3", "PATCH4")) {
            val result = when (connector().existsTable(name = tableName)) {
                true -> tableName
                else -> ""
            }
            actual.add(result)
        }
        assertEquals(expected, actual, "1回目のロールバック")
        assertEquals(version1.getVersionId(), migration.getCurrentVersionId(), "1回目のロールバック")

        migration.rollback()
        expected = listOf("", "", "", "")
        actual = mutableListOf()
        for (tableName in listOf("PATCH1", "PATCH2", "PATCH3", "PATCH4")) {
            val result = when (connector().existsTable(name = tableName)) {
                true -> tableName
                else -> ""
            }
            actual.add(result)
        }
        assertEquals(expected, actual, "2回目のロールバック")
        assertEquals("", migration.getCurrentVersionId(), "2回目のロールバック")
    }

    @Test
    fun `tag を指定して rollback を呼び出す`() {
        migration.initialize()
        migration.migrate(tags = arrayOf("tag1"))
        assertEquals(version2.getVersionId(), migration.getCurrentVersionId(), "migrate呼び出し後")
        migration.rollback(tags = arrayOf("tag1"))
        var expected = listOf("PATCH1", "PATCH2", "", "")
        var actual = mutableListOf<String>()
        for (tableName in listOf("PATCH1", "PATCH2", "PATCH3", "PATCH4")) {
            val result = when (connector().existsTable(name = tableName)) {
                true -> tableName
                else -> ""
            }
            actual.add(result)
        }
        assertEquals(expected, actual, "1回目のロールバック")
        assertEquals(version1.getVersionId(), migration.getCurrentVersionId(), "1回目のロールバック")

        migration.rollback()
        expected = listOf("", "", "", "")
        actual = mutableListOf()
        for (tableName in listOf("PATCH1", "PATCH2", "PATCH3", "PATCH4")) {
            val result = when (connector().existsTable(name = tableName)) {
                true -> tableName
                else -> ""
            }
            actual.add(result)
        }
        assertEquals(expected, actual, "2回目のロールバック")
        assertEquals("", migration.getCurrentVersionId(), "2回目のロールバック")
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
        migration = SQLMigration(
            connector = connector,
            module = module1,
        )
    }

    @AfterEach
    fun afterEach() {
        connector().use { it.createStatement().executeUpdate("SHUTDOWN") }
    }

    private val patch1 = SQLPatch(
        up = "CREATE TABLE PATCH1 (ID INT PRIMARY KEY NOT NULL, NAME VARCHAR(100) NOT NULL)",
        down = "DROP TABLE PATCH1",
        tag = "",
    )

    private val patchT1 = SQLPatch(
        up = "CREATE TRIGGER TRIGGER_PATCH1 BEFORE UPDATE ON PATCH1 REFERENCING NEW AS new_row FOR EACH ROW SET new_row.name = '';",
        down = "DROP TRIGGER TRIGGER_PATCH1",
        tag = "",
    )

    private val patch2 = SQLPatch(
        up = "CREATE TABLE PATCH2 (ID INT PRIMARY KEY NOT NULL, NAME VARCHAR(100) NOT NULL)",
        down = "DROP TABLE PATCH2",
        tag = "",
    )

    private val patchT2 = SQLPatch(
        up = "CREATE TRIGGER TRIGGER_PATCH2 BEFORE UPDATE ON PATCH2 REFERENCING NEW AS new_row FOR EACH ROW SET new_row.name = '';",
        down = "DROP TRIGGER TRIGGER_PATCH2",
        tag = "",
    )

    private val patch3 = SQLPatch(
        up = "CREATE TABLE PATCH3 (ID INT PRIMARY KEY NOT NULL, NAME VARCHAR(100) NOT NULL)",
        down = "DROP TABLE PATCH3",
        tag = "tag1",
    )

    private val patchT3 = SQLPatch(
        up = "CREATE TRIGGER TRIGGER_PATCH3 BEFORE UPDATE ON PATCH3 REFERENCING NEW AS new_row FOR EACH ROW SET new_row.name = '';",
        down = "DROP TRIGGER TRIGGER_PATCH3",
        tag = "tag1",
    )

    private val patch4 = SQLPatch(
        up = "CREATE TABLE PATCH4 (ID INT PRIMARY KEY NOT NULL, NAME VARCHAR(100) NOT NULL)",
        down = "DROP TABLE PATCH4",
        tag = "",
    )

    private val patchT4 = SQLPatch(
        up = "CREATE TRIGGER TRIGGER_PATCH4 BEFORE UPDATE ON PATCH4 REFERENCING NEW AS new_row FOR EACH ROW SET new_row.name = '';",
        down = "DROP TRIGGER TRIGGER_PATCH4",
        tag = "",
    )


    private val patch5 = SQLPatch(
        up = "CREATE TABLE PATCH5 (ID INT PRIMARY KEY NOT NULL, NAME VARCHAR(100) NOT NULL)",
        down = "DROP TABLE PATCH5",
        tag = "",
    )

    private val patchT5 = SQLPatch(
        up = "CREATE TRIGGER TRIGGER_PATCH5 BEFORE UPDATE ON PATCH5 REFERENCING NEW AS new_row FOR EACH ROW SET new_row.name = '';",
        down = "DROP TRIGGER TRIGGER_PATCH5",
        tag = "",
    )

    private val version1 = Version(
        versionId = "1.0.0",
        patch1,
        patchT1,
        patch2,
        patchT2,
    )

    private val version2 = Version(
        versionId = "2.0.0",
        patch3,
        patchT3,
        patch4,
        patchT4,
    )

    private val version3 = Version(
        versionId = "3.0.0",
        patch5,
        patchT5,
    )

    private val module1 = Module(
        moduleId = "sql",
        version1,
        version2,
    )

    private val module2 = Module(
        moduleId = "sql",
        version1,
        version2,
        version3,
    )

}