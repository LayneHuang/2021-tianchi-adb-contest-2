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
    // 线程数
    public static final int THREAD_COUNT = 10;
    // 每个线程的页数
    public static final int PAGE_COUNT = 1000;
    // 内存页每次申请的数组长度
    public static final int ARRAY_LENGTH = 1024 * 1024;
    // 文件页单次写入磁盘的页大小
    public static final int WRITE_SIZE = 8 * 1024;
    // MappedByteBuffer 单次读取的大小
     public static final long MAPPED_SIZE = 128 * 1024;

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
                        "_t" + page.threadIndex +
                        "_p" + page.pageIndex
        );
    }

    public static Path getPath(int tableIdx, int cIdx, int threadIdx, int pIdx) {
        return Constant.WORK_DIR.resolve(
                "t" + tableIdx +
                        "_c" + cIdx +
                        "_t" + threadIdx +
                        "_p" + pIdx
        );
    }
}
