package com.aliyun.adb.contest.page;

/**
 * @Classname Constant
 * @Description
 * @Date 2021/5/25 21:08
 * @Created by FinkyS
 */
public abstract class MyPage {

    public int size = 0;

    public long startValue;

    public long endValue;

    public long minValue = -1;

    public boolean isFake;

    public abstract void add(long value);

    public abstract void setMinValue(long value);

    public abstract long find(int index);

    public abstract long[] getValues();

}
