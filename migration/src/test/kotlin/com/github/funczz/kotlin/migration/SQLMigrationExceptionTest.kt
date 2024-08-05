package com.github.funczz.kotlin.migration

import com.github.funczz.kotlin.migration.model.Module
import com.github.funczz.kotlin.migration.model.Version
import com.github.funczz.kotlin.migration.model.patch.sql.SQLPatch
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.File
import java.sql.Connection
import java.sql.DriverManager

@Suppress("NonAsciiCharacters")
class SQLMigrationExceptionTest {

    @Test
    fun `DBにVERSIONテーブルがないなら getCurrentVersionId の呼び出しでIllegalVersionExceptionが発生する`() {
        val module = Module(
            moduleId = "module1",
            Version(
                versionId = "version1",
                SQLPatch(up = "", down = "")
            )
        )
        migration = SQLMigration(connector = connector, module = module)
        val actual = assertThrows<SQLMigration.IllegalVersionException> {
            migration.getCurrentVersionId()
        }
        assertEquals("Could not retrieve current version id.", actual.message)
    }

    @Test
    fun `DBにVERSIONテーブルがないなら migrate の呼び出しでIllegalVersionExceptionが発生する`() {
        val module = Module(
            moduleId = "module1",
            Version(
                versionId = "version1",
                SQLPatch(up = "", down = "")
            )
        )
        migration = SQLMigration(connector = connector, module = module)
        val actual = assertThrows<SQLMigration.IllegalVersionException> {
            migration.migrate()
        }
        assertEquals("Could not retrieve current version id.", actual.message)
    }

    @Test
    fun `DBにVERSIONテーブルがないなら rollback の呼び出しでIllegalVersionExceptionが発生する`() {
        val module = Module(
            moduleId = "module1",
            Version(
                versionId = "version1",
                SQLPatch(up = "", down = "")
            )
        )
        migration = SQLMigration(connector = connector, module = module)
        val actual = assertThrows<SQLMigration.IllegalVersionException> {
            migration.rollback()
        }
        assertEquals("Could not retrieve current version id.", actual.message)
    }

    @Test
    fun `ModuleにないVersionIdを migrate の呼び出しで渡したらIllegalVersionExceptionが発生する`() {
        val module = Module(
            moduleId = "module1",
            Version(
                versionId = "version1",
                SQLPatch(up = "", down = "")
            )
        )
        migration = SQLMigration(connector = connector, module = module)
        migration.initialize()
        val actual = assertThrows<SQLMigration.IllegalVersionException> {
            migration.migrate(versionId = "version2")
        }
        assertEquals("Module does not contain requested version id: requested version id=`version2`", actual.message)
    }

    @Test
    fun `VERSIONテーブルのversionIdがModuleにないなら migrate の呼び出しでIllegalVersionExceptionが発生する`() {
        SQLMigration(
            connector = connector,
            module = Module(
                moduleId = "module1",
                Version(
                    versionId = "versionA",
                    SQLPatch(
                        up = "CREATE TABLE PATCH1 (ID INT PRIMARY KEY NOT NULL, NAME VARCHAR(100) NOT NULL)",
                        down = ""
                    )
                )
            )
        ).let {
            it.initialize()
            it.migrate()
        }
        val module = Module(
            moduleId = "module1",
            Version(
                versionId = "version1",
                SQLPatch(up = "", down = "")
            )
        )
        migration = SQLMigration(connector = connector, module = module)
        assertEquals("versionA", migration.getCurrentVersionId())
        val actual = assertThrows<SQLMigration.IllegalVersionException> {
            migration.migrate()
        }
        assertEquals("Module does not contain current version id: current version id=`versionA`", actual.message)
    }

    @Test
    fun `VERSIONテーブルのversionIdがModuleにないなら rollback の呼び出しでIllegalVersionExceptionが発生する`() {
        SQLMigration(
            connector = connector,
            module = Module(
                moduleId = "module1",
                Version(
                    versionId = "versionA",
                    SQLPatch(
                        up = "CREATE TABLE PATCH1 (ID INT PRIMARY KEY NOT NULL, NAME VARCHAR(100) NOT NULL)",
                        down = ""
                    )
                )
            )
        ).let {
            it.initialize()
            it.migrate()
        }
        val module = Module(
            moduleId = "module1",
            Version(
                versionId = "version1",
                SQLPatch(up = "", down = "")
            )
        )
        migration = SQLMigration(connector = connector, module = module)
        assertEquals("versionA", migration.getCurrentVersionId())
        val actual = assertThrows<SQLMigration.IllegalVersionException> {
            migration.rollback()
        }
        assertEquals("Module does not contain current version id: current version id=`versionA`", actual.message)
    }

    private lateinit var migration: Migration

    private lateinit var connector: () -> Connection

    private val dbDir = File(".", "build/db/${this::class.simpleName}")

    @BeforeEach
    fun beforeEach() {
        if (dbDir.exists()) dbDir.deleteRecursively()
        connector = {
            DriverManager.getConnection("jdbc:hsqldb:file:$dbDir/test_db;shutdown=true", "sa", "sa").also {
                it.autoCommit = false
            }
        }
    }

    @AfterEach
    fun afterEach() {
        connector().use { it.createStatement().executeUpdate("SHUTDOWN") }
    }
}
