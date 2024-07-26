package com.github.funczz.kotlin.migration.util.sql

import java.sql.Connection
import java.sql.ResultSet
import java.util.*

interface SQLConnectionExt {

    fun Connection.update(sql: String, vararg params: Any): Int {
        this.prepareStatement(sql).use { stmt ->

            for (i in params.indices) {
                when (val param = params[i]) {
                    is Int -> {
                        stmt.setInt(i + 1, (param))
                    }

                    is String -> {
                        stmt.setString(i + 1, param)
                    }

                    else -> {
                    }
                }
            }

            return stmt.executeUpdate()
        }
    }

    fun <R : Any> Connection.select(
        sql: String,
        vararg params: Any,
        function: (ResultSet) -> R
    ): Optional<R> {
        this.prepareStatement(sql).use { stmt ->

            for (i in params.indices) {
                when (val param = params[i]) {
                    is Int -> {
                        stmt.setInt(i + 1, (param))
                    }

                    is String -> {
                        stmt.setString(i + 1, param)
                    }

                    else -> {
                    }
                }
            }

            stmt.executeQuery().use {
                return if (it.next()) {
                    Optional.ofNullable(function(it))
                } else {
                    Optional.empty()
                }
            }
        }
    }

    fun Connection.existsTable(name: String): Boolean {
        try {
            val rs: ResultSet = this.metaData.getTables(
                null,
                null,
                "%",
                arrayOf("TABLE")
            )
            rs.use {
                while (it.next()) {
                    val tableName = it.getString("TABLE_NAME")
                    if (tableName.uppercase(Locale.getDefault()) == name.uppercase(Locale.getDefault())) {
                        return true
                    }
                }
            }
        } catch (_: Exception) {
        }
        return false
    }

    fun Connection.existsVersionsTable(): Boolean {
        return this.existsTable(name = "VERSIONS")
    }

    fun <R> Connection.commit(transactionIsolation: Int? = null, function: (Connection) -> R): R {
        val originAutoCommit = this.autoCommit
        val originTransactionIsolation = this.transactionIsolation
        return try {
            this.autoCommit = false
            transactionIsolation?.let {
                this.transactionIsolation = it
            }
            val result = function(this)
            this.commit()
            result
        } catch (th: Throwable) {
            this.rollback()
            throw th
        } finally {
            try {
                if (originAutoCommit) this.autoCommit = true
                if (originTransactionIsolation != this.transactionIsolation) {
                    this.transactionIsolation = originTransactionIsolation
                }
            } catch (_: Throwable) {
            } finally {
                try {
                    this.close()
                } catch (_: Throwable) {
                }
            }
        }
    }
}