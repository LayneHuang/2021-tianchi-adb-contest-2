package com.aliyun.adb.contest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Classname Constant
 * @Description
 * @Date 2021/5/25 21:08
 * @Created by FinkyS
 */
public class MyMemoryPage extends MyPage{

    public List<long[]> arraysList = new ArrayList<>();
    public long[] sortedArrays;
    public long[] currentArrays;
    public int index;

    @Override
    public void add(long value) {
        if (currentArrays == null){
            currentArrays = new long[Constant.ARRAY_LENGTH];
            arraysList.add(currentArrays);
        }
        currentArrays[index++] = value;
        size++;
        if (index == Constant.ARRAY_LENGTH - 1) {
            currentArrays = null;
            index = 0;
        }
    }

    @Override
    long find(int index) {
        if (sortedArrays == null){
            sortedArrays = new long[size];
            int tempIndex = 0;
            for (long[] longs : arraysList) {
                for (int i = 0; i < longs.length && tempIndex < size; i++) {
                    sortedArrays[tempIndex++] = longs[i];
                }
            }
            Arrays.parallelSort(sortedArrays);
        }
        return sortedArrays[index];
    }

    @Override
    long[] getValues() {
        long[] values = new long[size];
        int tempIndex = 0;
        for (long[] longs : arraysList) {
            for (int i = 0; i < longs.length && tempIndex < size; i++) {
                values[tempIndex++] = longs[i];
            }
        }
        return values;
    }

    public MyMemoryPage(long startValue, long endValue) {
        this.startValue = startValue;
        this.endValue = endValue;
    }
}
