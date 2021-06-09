package com.aliyun.adb.contest.page;

import com.aliyun.adb.contest.Constant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Classname Constant
 * @Description
 * @Date 2021/5/25 21:08
 * @Created by FinkyS
 */
public final class MyMemoryPage extends MyPage {

    public List<long[]> arraysList = new ArrayList<>();
    public long[] sortedArrays;
    public long[] currentArrays;
    public int index;

    @Override
    public void add(long value) {
//        if (isFake){
//            size++;
//            return;
//        }
        if (currentArrays == null) {
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

    public void setMinValue(long value) {
        if (value < minValue) minValue = value;
        size++;
    }

    @Override
    public long find(int index) {
        if (sortedArrays == null) {
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
    public long[] getValues() {
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