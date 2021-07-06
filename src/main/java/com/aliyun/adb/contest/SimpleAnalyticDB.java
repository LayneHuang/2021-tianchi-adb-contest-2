package com.aliyun.adb.contest;

import com.aliyun.adb.contest.page.MyFilePage;
import com.aliyun.adb.contest.spi.AnalyticDB;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SimpleAnalyticDB implements AnalyticDB {
    private MyFileReader[] myFileReaders;
    private MyFileWriter[] myFileWriters;

    /**
     * The implementation must contain a public no-argument constructor.
     */
    public SimpleAnalyticDB() {
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        long t = System.currentTimeMillis();
        Constant.WORK_DIR = Paths.get(workspaceDir);
        Path dirPath = Paths.get(tpchDataFileDir);
        Files.list(dirPath).forEach(path -> {
            if (path.getFileName().toString().equals("results")) {
                // 跳过结果数据
                return;
            }
            myFileReaders = new MyFileReader[Constant.THREAD_COUNT];
            myFileWriters = new MyFileWriter[Constant.THREAD_COUNT];
            for (int i = 0; i < Constant.THREAD_COUNT; i++) {
                myFileReaders[i] = new MyFileReader(path, i, Constant.THREAD_COUNT, Constant.MAPPED_SIZE, Constant.PAGE_COUNT);
                myFileWriters[i] = new MyFileWriter(myFileReaders[i]);
            }
            for (int i = 0; i < Constant.THREAD_COUNT; i++) {
                if (i < Constant.THREAD_COUNT - 1) {
                    myFileWriters[i].nextMyFileWriter = myFileWriters[i + 1];
                }
                myFileReaders[i].start();
                myFileWriters[i].start();
            }
            for (int i = 0; i < Constant.THREAD_COUNT; i++) {
                try {
                    myFileReaders[i].join();
                    myFileWriters[i].join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        System.out.println("COST TIME : " + (System.currentTimeMillis() - t));
    }

    private int quantileCount = 0;

    @Override
    public String quantile(String table, String column, double percentile) {
        if (quantileCount++ > 8) {
            return "";
        }
        String ans = String.valueOf(MyPageManager.find(column, percentile, myFileWriters));
        System.out.println("Query:" + table + ", " + column + ", " + percentile + " Answer:" + ans);
        return ans;
    }

}
