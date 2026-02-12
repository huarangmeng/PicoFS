package com.hrm.fs.core

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * 简单的 LRU 缓存。
 *
 * 使用 [LinkedHashMap] 保持插入/访问顺序，手动在 get/put 时维护 LRU 语义。
 * Kotlin/Native 的 LinkedHashMap 不支持 accessOrder 构造参数且不可继承，
 * 因此通过 remove+put 模拟"最近访问移到末尾"。
 *
 * 线程安全（通过协程 [Mutex]），可在协程上下文中调用。
 * 相比 `SynchronizedObject`，`Mutex` 是挂起式锁，不阻塞线程。
 *
 * @param maxSize 最大条目数，超出时淘汰最久未访问的条目
 */
internal class VfsCache<K, V>(private val maxSize: Int) {

    private val mutex = Mutex()
    private val map = LinkedHashMap<K, V>()

    /** 获取缓存值，不存在则返回 null。命中时将条目提升至最近使用。 */
    suspend fun get(key: K): V? = mutex.withLock {
        val value = map.remove(key) ?: return@withLock null
        map[key] = value   // re-insert → moves to tail (most recent)
        value
    }

    /** 写入缓存，超出容量时淘汰最久未访问的条目。 */
    suspend fun put(key: K, value: V) = mutex.withLock {
        map.remove(key)    // remove first to ensure re-insert at tail
        map[key] = value
        if (map.size > maxSize) {
            val eldest = map.keys.iterator().next()
            map.remove(eldest)
        }
    }

    /** 移除指定 key 的缓存。 */
    suspend fun remove(key: K) { mutex.withLock { map.remove(key) } }

    /**
     * 移除所有以 [prefix] 开头的 key（适用于 String key）。
     * 用于目录变更时批量失效子路径缓存。
     *
     * 使用迭代器原地删除，避免创建中间列表。
     */
    suspend fun removeByPrefix(prefix: K) = mutex.withLock {
        val prefixStr = prefix.toString()
        val iter = map.keys.iterator()
        while (iter.hasNext()) {
            if (iter.next().toString().startsWith(prefixStr)) {
                iter.remove()
            }
        }
    }

    /** 清空所有缓存。 */
    suspend fun clear() { mutex.withLock { map.clear() } }

    /** 当前缓存条目数。 */
    suspend fun size(): Int = mutex.withLock { map.size }
}
