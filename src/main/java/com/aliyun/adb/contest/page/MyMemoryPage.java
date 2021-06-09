package com.aliyun.adb.contest.page;

import com.aliyun.adb.contest.Constant;

import java.util.Arrays;

/**
 * @Classname Constant
 * @Description
 * @Date 2021/5/25 21:08
 * @Created by FinkyS
 */
public final class MyMemoryPage extends MyPage {

    public long[] arraysList;
    private boolean firstRead;

    @Override
    public void add(long value) {
        if (arraysList == null) {
            arraysList = new long[Constant.ARRAY_LENGTH];
        } else if (size >= arraysList.length) {
            arraysList = Arrays.copyOf(
                    arraysList,
                    arraysList.length + (arraysList.length >> 1)
            );
        }
        arraysList[size++] = value;
    }

    public void setMinValue(long value) {
        if (value < minValue) minValue = value;
        size++;
    }

    @Override
    public long find(int index) {
        return 0;
    }

    @Override
    public long[] getValues() {
        if (!firstRead) {
            firstRead = true;
            long[] result = new long[size];
            if (size >= 0) System.arraycopy(arraysList, 0, result, 0, size);
            arraysList = result;
        }
        return arraysList;
    }

    public MyMemoryPage(long startValue, long endValue) {
        this.startValue = startValue;
        this.endValue = endValue;
    }
}
