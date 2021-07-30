package com.aliyun.adb.contest.page;

/**
 * MyPage
 *
 * @author 86188
 * @since 2021/7/30
 */
public abstract class MyPage {

    public int dataCount;

    protected int size;

    public abstract void add(long value);

    public abstract boolean full();

    public abstract void clean();

    public abstract Object getData();

    public boolean empty() {
        return size == 0;
    }
}
