package com.aliyun.adb.contest.cache;

import java.nio.MappedByteBuffer;

/**
 * @Classname MyBufferCache
 * @Description
 * @Date 2021/6/3 16:38
 * @Created by FinkyS
 */
public abstract class MyCache {

    public abstract MappedByteBuffer poll() throws InterruptedException;

    public abstract void put(MappedByteBuffer buffer) throws InterruptedException;
}
