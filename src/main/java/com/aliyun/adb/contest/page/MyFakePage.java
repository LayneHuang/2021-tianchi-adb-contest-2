package com.aliyun.adb.contest.page;

/**
 * @Classname Constant
 * @Description
 * @Date 2021/5/25 21:08
 * @Created by FinkyS
 */
public class MyFakePage extends MyPage{

    @Override
    public void add(long value) {
        size++;
    }

    @Override
    public long find(int index) {
        return 0;
    }

    @Override
    public long[] getValues() {
        return new long[size];
    }

    public MyFakePage(long startValue, long endValue) {
        this.startValue = startValue;
        this.endValue = endValue;
    }
}
