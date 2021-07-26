package com.aliyun.adb.contest.cache;

import java.nio.ByteBuffer;

/**
 * 瞎几把缓存一下
 *
 * @author 86188
 * @since 2021/7/26
 */
public class MemoryCache {

    private int size = 0;

    private long[] data;

    public void trans(ByteBuffer buffer) {
        size = buffer.position() / Long.BYTES;
        data = new long[size];
        int idx = 0;
        buffer.flip();
        while (buffer.hasRemaining()) {
            data[idx++] = buffer.getLong();
        }
    }

    public static MemoryCache gen(ByteBuffer buffer) {
        MemoryCache cache = new MemoryCache();
        cache.trans(buffer);
        return cache;
    }

    public int getSize() {
        return size;
    }

    public long[] getData() {
        return data;
    }
}
