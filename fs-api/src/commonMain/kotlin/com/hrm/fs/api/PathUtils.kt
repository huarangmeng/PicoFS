package com.hrm.fs.api

object PathUtils {
    fun normalize(path: String): String {
        if (path.isBlank()) return "/"
        val parts = path.split("/").filter { it.isNotBlank() }
        val stack = ArrayDeque<String>()
        for (p in parts) {
            when (p) {
                "." -> Unit
                ".." -> if (stack.isNotEmpty()) stack.removeLast()
                else -> stack.addLast(p)
            }
        }
        return "/" + stack.joinToString("/")
    }
}
