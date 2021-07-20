package com.aliyun.adb.contest;

import com.aliyun.adb.contest.page.MyValuePage;

import java.nio.file.Path;

/**
 * @Classname Constant
 * @Description
 * @Date 2021/5/25 21:08
 * @Created by FinkyS
 */
public final class Constant {
    public static Path WORK_DIR;
    public static final boolean IS_DEBUG = false;
    public static final int DATA_SIZE = IS_DEBUG ? 10000 : 1000000000;
    // 线程数
    public static final int THREAD_COUNT = 16;
    // 每个线程的页数
    public static final int PAGE_COUNT = 1000;
    // 内存页每次申请的数组长度
    public static final int ARRAY_LENGTH = 1024 * 1024;
    // 文件页单次写入磁盘的页大小
    public static final int WRITE_SIZE = 32 * 1024;
    // MappedByteBuffer 单次读取的大小
    public static final int MAPPED_SIZE = 256 * 1024 * 1024;

    public static void main(String[] args) {
        long halfFile = (long) (WRITE_SIZE * THREAD_COUNT * PAGE_COUNT) + (ARRAY_LENGTH * PAGE_COUNT * THREAD_COUNT * 8L);
        long allFile = (WRITE_SIZE * THREAD_COUNT * PAGE_COUNT * 2L);
        System.out.println("内存硬盘各存一半时内存大概占用:" + halfFile / 1024 / 1024.f + " MB");
        System.out.println("纯硬盘存时内存大概占用:" + allFile / 1024 / 1024.f + " MB");
    }

    public static int getPageIndex(int value) {
        int distance = Integer.MAX_VALUE / PAGE_COUNT;
        return Math.min(PAGE_COUNT - 1, value / distance);
    }

    public static int getPageIndex(long value) {
        long distance = Long.MAX_VALUE / PAGE_COUNT;
        long result = (value / distance);
        if (result < 0) System.out.println("FUCK: " + value + " " + distance);
        return (int) Math.min(PAGE_COUNT - 1, (value / distance));
    }

    public static int getPageIndex(double value) {
        double distance = Double.MAX_VALUE / PAGE_COUNT;
        return (int) Math.min(PAGE_COUNT - 1, (value / distance));
    }

    public static Path getPath(MyValuePage page) {
        return Constant.WORK_DIR.resolve(
                "t" + page.tableIndex +
                        "_c" + page.columnIndex +
                        "_b" + page.blockIndex +
                        "_p" + page.pageIndex
        );
    }

    public static Path getPath(int tIdx, int cIdx, int bIdx, int pIdx) {
        return Constant.WORK_DIR.resolve(
                "t" + tIdx +
                        "_c" + cIdx +
                        "_b" + bIdx +
                        "_p" + pIdx
        );
    }
}
