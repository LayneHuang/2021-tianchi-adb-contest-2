package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SimpleAnalyticDB implements AnalyticDB {

    public static boolean TIME_OUT = false;
    private MyFileReader[] myFileReaders;

    /**
     * The implementation must contain a public no-argument constructor.
     */
    public SimpleAnalyticDB() {
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        long t = System.currentTimeMillis();
        MyFilePage.WORK_DIR = Paths.get(workspaceDir);
        Path dirPath = Paths.get(tpchDataFileDir);
        Files.list(dirPath).forEach(path -> {
            if (path.getFileName().toString().equals("results")) {
                // 跳过结果数据
                return;
            }
            myFileReaders = new MyFileReader[Constant.THREAD_COUNT];
            for (int i = 0; i < Constant.THREAD_COUNT; i++) {
                myFileReaders[i] = new MyFileReader(path, i, Constant.THREAD_COUNT, Constant.MAPPED_SIZE, Constant.PAGE_COUNT);
            }
            for (int i = 0; i < Constant.THREAD_COUNT; i++) {
                if (i < Constant.THREAD_COUNT - 1) {
                    myFileReaders[i].nextMyFileReader = myFileReaders[i + 1];
                }
                myFileReaders[i].start();
            }
            for (int i = 0; i < Constant.THREAD_COUNT; i++) {
                try {
                    myFileReaders[i].join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        System.out.println("COST TIME : " + (System.currentTimeMillis() - t));
        // 测试可能命中的所有page下标
//        for (int i = 0; i < 101; i++) {
//            MyPageManager.find(MyPageManager.tableColumnKeys[0], 0.01 * i, myFileReaders);
//        }
//        for (int i = 0; i < 101; i++) {
//            MyPageManager.find(MyPageManager.tableColumnKeys[1], 0.01 * i, myFileReaders);
//        }
    }

    private int quantileCount = 0;

    @Override
    public String quantile(String table, String column, double percentile) {
//        return "";
//        if (quantileCount++ > 8) {
//            return "";
//        }
        String ans = String.valueOf(MyPageManager.find(column, percentile, myFileReaders));
        System.out.println("Query:" + table + ", " + column + ", " + percentile + " Answer:" + ans);
        return ans;
    }

}
