package com.aliyun.adb.contest;

/**
 * @Classname Constant
 * @Description
 * @Date 2021/5/25 21:08
 * @Created by FinkyS
 */
public class Constant {
    // 数据量
    public static final int LINE_COUNT = 3_00_000_000;
    // 线程数
    public static final int THREAD_COUNT = 4;
    // 每个线程的页数
    public static final int PAGE_COUNT = 1000;
    // 内存页每次申请的数组长度
    public static final int ARRAY_LENGTH = (int) (LINE_COUNT / PAGE_COUNT / THREAD_COUNT * 1.1);
    // 文件页单次写入磁盘的页大小
    public static final int WRITE_SIZE = 32 * 1024;
    // MappedByteBuffer 单次读取的大小
    public static final int MAPPED_SIZE = 64 * 1024 * 1024;

    public static void main(String[] args) {
        long halfFile = (long)(WRITE_SIZE*THREAD_COUNT*PAGE_COUNT)+(ARRAY_LENGTH*PAGE_COUNT*THREAD_COUNT*8L);
        long allFile = (WRITE_SIZE*THREAD_COUNT*PAGE_COUNT*2L);
        System.out.println("内存硬盘各存一半时内存大概占用:"+halfFile/1024/1024.f+" MB");
        System.out.println("纯硬盘存时内存大概占用:"+allFile/1024/1024.f+" MB");
    }
}
