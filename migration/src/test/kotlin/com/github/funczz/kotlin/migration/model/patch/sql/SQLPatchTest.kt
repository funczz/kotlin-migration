package com.github.funczz.kotlin.migration.model.patch.sql

import com.github.funczz.kotlin.junit5.Cases
import com.github.funczz.kotlin.migration.SQLMigration
import com.github.funczz.kotlin.migration.util.sql.SQLConnectionExt
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import java.sql.Connection
import java.sql.DriverManager

@Suppress("NonAsciiCharacters")
class SQLPatchTest : Cases, SQLConnectionExt {

    @Test
    fun `SQLPatch を比較する`() {
        val sqlPatch1 = SQLPatch(up = "", down = "", tag = "")
        val sqlPatch2 = SQLPatch(up = "", down = "", tag = "")
        val sqlPatch3 = SQLPatch(up = "", down = "", tag = "tag3")
        assertEquals(sqlPatch1, sqlPatch2)
        assertNotEquals(sqlPatch2, sqlPatch3)
    }

    @Test
    fun `toString で文字列を取得する`() {
        val sqlPatch = SQLPatch(up = "", down = "", tag = "tag1")
        assertEquals("SQLPatch(tag=tag1)", sqlPatch.toString())
    }

    @TestFactory
    fun `migrate と rollback　を順に呼び出す`() = casesDynamicTest(
        DriverManager.getConnection("jdbc:hsqldb:mem:test_db", "sa", "sa"),
        DriverManager.getConnection("jdbc:h2:mem:test_db", "sa", "sa"),
    ) { conn ->
        connection = conn
        val patch = SQLPatch(
            up = "CREATE TABLE PERSON (PERSON_ID INT PRIMARY KEY NOT NULL, PERSON_NAME VARCHAR(100) NOT NULL)",
            down = "DROP TABLE PERSON",
        )
        context[SQLMigration.CONNECTION] = connection
        patch.migrate(moduleId = "", versionId = "", tags = arrayOf(), context)
        assertTrue(connection.existsTable(name = "PERSON"), "expected: PERSON table is created.")

        patch.rollback(moduleId = "", versionId = "", tags = arrayOf(), context)
        assertFalse(connection.existsTable(name = "PERSON"), "expected: PERSON table is dropped.")
    }

    @TestFactory
    fun `tag を指定して migrate を呼び出す`() = casesDynamicTest(
        Pair("", true),
        Pair("match", true),
        Pair("mismatch", false),
    ) { (tag, expected) ->
        connection = DriverManager.getConnection("jdbc:hsqldb:mem:test_db", "sa", "sa")
        val patch = SQLPatch(
            up = "CREATE TABLE PERSON (PERSON_ID INT PRIMARY KEY NOT NULL, PERSON_NAME VARCHAR(100) NOT NULL)",
            down = "DROP TABLE PERSON",
            tag = tag,
        )
        context[SQLMigration.CONNECTION] = connection
        patch.migrate(moduleId = "", versionId = "", tags = arrayOf("match", "tag1", "tag2"), context)
        assertEquals(expected, connection.existsTable(name = "PERSON"), "expected: PERSON table exists = $expected.")
    }

    @TestFactory
    fun `リソースファイルを指定して SQLPatch を生成する`() = casesDynamicTest(
        Triple("db/migration/v1_0_0/patch1_up.sql", "db/migration/v1_0_0/patch1_down.sql", "PATCH1"),
    ) { (up, down, table) ->
        connection = DriverManager.getConnection("jdbc:hsqldb:mem:test_db", "sa", "sa")
        val patch = SQLPatch(
            classLoader = Thread.currentThread().contextClassLoader,
            up = up,
            down = down,
            tag = "",
        )
        context[SQLMigration.CONNECTION] = connection
        patch.migrate(moduleId = "", versionId = "", tags = arrayOf(), context)
        assertEquals(true, connection.existsTable(name = table), "expected: $table table exists = true.")

        patch.rollback(moduleId = "", versionId = "", tags = arrayOf(), context)
        assertEquals(false, connection.existsTable(name = table), "expected: $table table exists = false.")
    }

    private lateinit var connection: Connection

    private lateinit var context: MutableMap<String, Any>

    override fun setUpCases() {
        context = mutableMapOf()
    }

    override fun tearDownCases() {
        connection.use { it.createStatement().executeUpdate("SHUTDOWN") }
    }

}