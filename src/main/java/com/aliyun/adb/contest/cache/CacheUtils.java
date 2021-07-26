package com.aliyun.adb.contest.cache;

import com.aliyun.adb.contest.Constant;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * CacheUtils
 *
 * @author 86188
 * @since 2021/7/26
 */
public class CacheUtils {

    private static final MemoryCache[][] caches = new MemoryCache[Constant.THREAD_COUNT][Constant.CACHE_SIZE];

    private static final int[] size = new int[Constant.THREAD_COUNT];

    private static final Map<String, Integer> map = new HashMap<>();

    public static void cache(int threadId, int tableIdx, int cIdx, int pIdx, ByteBuffer buffer) {
        if (size[threadId] >= Constant.CACHE_SIZE || buffer.position() == 0) return;
        int result = size[threadId];
        caches[threadId][size[threadId]++] = MemoryCache.gen(buffer);
        map.put(Constant.getPathStr(threadId, tableIdx, cIdx, pIdx), result);
    }

    public static MemoryCache get(int threadId, int tableIdx, int cIdx, int pIdx) {
        int pos = map.getOrDefault(Constant.getPathStr(threadId, tableIdx, cIdx, pIdx), -1);
        if (pos == -1) return null;
        return caches[threadId][pos];
    }

}
