package com.aliyun.adb.contest;

import java.util.Arrays;

/**
 * @Classname MyPageManager
 * @Description
 * @Date 2021/5/26 10:41
 * @Created by FinkyS
 */
public class MyPageManager {

    public static String[] tableColumnKeys = {"L_ORDERKEY", "L_PARTKEY"};

    private static long getValue(MyFileReader[] myFileReader, int pageIndex, int colIndex, int insideIndex) {
        int size = 0;
        for (int i = 0; i < Constant.THREAD_COUNT; i++) {
            size += myFileReader[i].pages[colIndex][pageIndex].size;
        }
        long[] sortedArrays = new long[size];
        int addIndex = 0;
        for (int i = 0; i < Constant.THREAD_COUNT; i++) {
            long[] values = myFileReader[i].pages[colIndex][pageIndex].getValues();
            for (long value : values) {
                sortedArrays[addIndex++] = value;
            }
        }
        Arrays.parallelSort(sortedArrays);
        return sortedArrays[insideIndex];
    }

    public static long find(String column, double percentile, MyFileReader[] myFileReader) {
        int colIndex = 0;
        if (column.equals(tableColumnKeys[1])) {
            colIndex = 1;
        }
        int index = (int) Math.round(Constant.LINE_COUNT * percentile) - 1;
        if (index < 0) {
            index = 0;
        }
        int nowMaxIndex = 0;
        int insideIndex = index;
        for (int i = 0; i < Constant.PAGE_COUNT; i++) {
            for (int j = 0; j < Constant.THREAD_COUNT; j++) {
                MyPage selectedPage = myFileReader[j].pages[colIndex][i];
                nowMaxIndex += selectedPage.size;
                if (nowMaxIndex > index) {
                    System.out.println(i);
                    return getValue(myFileReader, i, colIndex, insideIndex);
                }
            }
            for (int j = 0; j < Constant.THREAD_COUNT; j++) {
                MyPage selectedPage = myFileReader[j].pages[colIndex][i];
                insideIndex -= selectedPage.size;
            }
        }
        return -1;
    }
}
