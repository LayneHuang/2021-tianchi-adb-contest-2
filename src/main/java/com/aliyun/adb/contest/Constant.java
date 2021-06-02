package com.aliyun.adb.contest;

/**
 * @Classname Constant
 * @Description
 * @Date 2021/5/25 21:08
 * @Created by FinkyS
 */
public class Constant {
    // 数据量
    public static int LINE_COUNT = 3_00_000_000;
    // 线程数
    public static int THREAD_COUNT = 4;
    // 每个线程的页数
    public static int PAGE_COUNT = 1000;
    // 内存页每次申请的数组长度
    public static int ARRAY_LENGTH = (int) (LINE_COUNT / PAGE_COUNT / THREAD_COUNT * 1.1);
    // 文件页单次写入磁盘的页大小
    public static int WRITE_SIZE = 256 * 1024;
}
