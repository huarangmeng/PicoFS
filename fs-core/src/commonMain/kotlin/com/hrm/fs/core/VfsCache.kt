package com.hrm.fs.core

import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.internal.SynchronizedObject
import kotlinx.coroutines.internal.synchronized

/**
 * 简单的 LRU 缓存。
 *
 * 使用 [LinkedHashMap] 保持插入/访问顺序，手动在 get/put 时维护 LRU 语义。
 * Kotlin/Native 的 LinkedHashMap 不支持 accessOrder 构造参数且不可继承，
 * 因此通过 remove+put 模拟"最近访问移到末尾"。
 *
 * 线程安全（通过 synchronized），不使用协程锁，可在任意上下文中调用。
 *
 * @param maxSize 最大条目数，超出时淘汰最久未访问的条目
 */
@OptIn(InternalCoroutinesApi::class)
internal class VfsCache<K, V>(private val maxSize: Int) {

    private val lock = SynchronizedObject()
    private val map = LinkedHashMap<K, V>()

    /** 获取缓存值，不存在则返回 null。命中时将条目提升至最近使用。 */
    fun get(key: K): V? = synchronized(lock) {
        val value = map.remove(key) ?: return@synchronized null
        map[key] = value   // re-insert → moves to tail (most recent)
        value
    }

    /** 写入缓存，超出容量时淘汰最久未访问的条目。 */
    fun put(key: K, value: V) = synchronized(lock) {
        map.remove(key)    // remove first to ensure re-insert at tail
        map[key] = value
        if (map.size > maxSize) {
            val eldest = map.keys.iterator().next()
            map.remove(eldest)
        }
    }

    /** 移除指定 key 的缓存。 */
    fun remove(key: K) = synchronized(lock) { map.remove(key) }

    /**
     * 移除所有以 [prefix] 开头的 key（适用于 String key）。
     * 用于目录变更时批量失效子路径缓存。
     */
    fun removeByPrefix(prefix: K) = synchronized(lock) {
        val prefixStr = prefix.toString()
        val keysToRemove = map.keys.filter { it.toString().startsWith(prefixStr) }
        keysToRemove.forEach { map.remove(it) }
    }

    /** 清空所有缓存。 */
    fun clear() = synchronized(lock) { map.clear() }

    /** 当前缓存条目数。 */
    val size: Int get() = synchronized(lock) { map.size }
}
