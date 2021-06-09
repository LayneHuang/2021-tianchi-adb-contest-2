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

    public long[] arrays;
    private boolean firstRead;

    @Override
    public void add(long value) {
        if (arrays == null) {
            arrays = new long[Constant.ARRAY_LENGTH];
        } else if (size >= arrays.length) {
            arrays = Arrays.copyOf(
                    arrays,
                    arrays.length + (arrays.length >> 1)
            );
        }
        arrays[size++] = value;
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
            if (size >= 0) System.arraycopy(arrays, 0, result, 0, size);
            arrays = result;
        }
        return arrays;
    }

    public MyMemoryPage(long startValue, long endValue) {
        this.startValue = startValue;
        this.endValue = endValue;
    }
}
