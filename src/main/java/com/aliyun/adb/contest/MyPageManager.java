package com.aliyun.adb.contest;

import com.aliyun.adb.contest.page.MyTable;

public final class MyPageManager {

    public static long find(MyTable table, String column, double percentile) {
        long rank = Math.round(Constant.LINE_COUNT * percentile) - 1;
        long offset = 0;
        for (int i = 0; i < Constant.PAGE_COUNT; i++) {

        }

        return -1;
    }
}
