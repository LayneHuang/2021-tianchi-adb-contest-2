package com.aliyun.adb.contest.cache;

import com.aliyun.adb.contest.Constant;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DataCache
 *
 * @author 86188
 * @since 2021/7/29
 */
public class DataCache {

    private final Map<String, Integer> posMap = new HashMap<>();

    private final long[][] cache = new long[Constant.CACHE_SIZE][];

    private final AtomicInteger counter = new AtomicInteger();

    public void cache(int tIdx,
                      int cIdx,
                      int pIdx, long[] data) {
        if (counter.get() >= Constant.CACHE_SIZE) return;
        int pos = counter.getAndIncrement();
        if (pos >= Constant.CACHE_SIZE) return;
        posMap.put(getKey(tIdx, cIdx, pIdx), pos);
        cache[pos] = data;
    }

    public long[] query(int tIdx,
                        int cIdx,
                        int pIdx) {
        String key = getKey(tIdx, cIdx, pIdx);
        if (!posMap.containsKey(key)) return null;
        System.out.println("GET FROM CACHE: " + key);
        int pos = posMap.get(key);
        return cache[pos];
    }

    private String getKey(int tIdx,
                          int cIdx,
                          int pIdx) {
        return tIdx + "-" + cIdx + "-" + pIdx;
    }

}
