package com.github.funczz.kotlin.migration.model

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test

@Suppress("NonAsciiCharacters")
class VersionTest {

    @Test
    fun `Version を比較する`() {
        val version1 = Version(versionId = "version")
        val version2 = Version(versionId = "version")
        val version3 = Version(versionId = "version3")
        assertEquals(version1, version2)
        assertNotEquals(version2, version3)
    }

    @Test
    fun `toString で文字列を取得する`() {
        val version = Version(versionId = "version")
        assertEquals("Version(versionId=version, patches=)", version.toString())
    }
}