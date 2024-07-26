package com.github.funczz.kotlin.migration.model

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test

@Suppress("NonAsciiCharacters")
class ModuleTest {

    @Test
    fun `Module を比較する`() {
        val module1 = Module(moduleId = "module")
        val module2 = Module(moduleId = "module")
        val module3 = Module(moduleId = "module3")
        assertEquals(module1, module2)
        assertNotEquals(module2, module3)
    }

    @Test
    fun `toString で文字列を取得する`() {
        val module = Module(moduleId = "module")
        assertEquals("Module(moduleId=module, versions=)", module.toString())
    }
}