package com.aliyun.adb.contest;

import java.nio.file.Path;

public final class Constant {
    public static Path WORK_DIR;
    // 线程数
    public static final int THREAD_COUNT = 10;
    // 每个线程的页数
    public static final int PAGE_COUNT = 1000;
    // 文件页单次写入磁盘的页大小
    public static final int WRITE_SIZE = 64 * 1024;

    public static final int WRITE_COUNT = WRITE_SIZE / Long.BYTES;
    // MappedByteBuffer 单次读取的大小
    public static final long MAPPED_SIZE = 32 * 1024 * 1024;
    // 列数目
    public static final int MAX_COL_COUNT = 2;

    // 数据数量
    public static final int DATA_COUNT = 1000_000_000;

    // CacheSize
    public static final int CACHE_SIZE = (int) ((long) (6.5 * 1024L * 1024 * 1024) / (DATA_COUNT / PAGE_COUNT * Long.BYTES));

    public static final long DISTANCE = Long.MAX_VALUE / PAGE_COUNT;

    public static int getPageIndex(long value) {
        return (int) Math.min(PAGE_COUNT - 1, (value / DISTANCE));
    }

    public static Path getGlobalPath() {
        return Constant.WORK_DIR.resolve("a_table_info");
    }

    public static Path getPath(int threadIdx, int tableIdx, int cIdx, int pIdx) {
        return Constant.WORK_DIR.resolve(
                getPathStr(threadIdx, tableIdx, cIdx, pIdx)
        );
    }

    public static String getPathStr(int threadIdx, int tableIdx, int cIdx, int pIdx) {
        return "t" + tableIdx + "_c" + cIdx + "_t" + threadIdx + "_p" + pIdx;
    }
}
