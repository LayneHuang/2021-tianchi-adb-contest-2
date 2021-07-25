package com.aliyun.adb.contest;

import com.aliyun.adb.contest.page.MyTable;
import com.aliyun.adb.contest.pool.ReadThread;
import com.aliyun.adb.contest.pool.WriteThread;
import com.aliyun.adb.contest.spi.AnalyticDB;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SimpleAnalyticDB implements AnalyticDB {
    private List<MyTable> tables = new ArrayList<>();
    private final Map<String, Integer> indexMap = new HashMap<>();
    private static final ReadThread[] rThreads = new ReadThread[Constant.THREAD_COUNT];
    private static final WriteThread[] wThreads = new WriteThread[Constant.THREAD_COUNT];

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
        List<Path> tablePaths = Files.list(dirPath).collect(Collectors.toList());
        // 等待所有表跑完
        System.out.printf("table count: %d\n", tablePaths.size());
        int tableIndex = 0;
        for (Path path : tablePaths) {
            if (path.getFileName().toString().equals("results")) {
                // 跳过结果数据
                continue;
            }
            MyTable table = new MyTable();
            table.index = tableIndex;
            table.path = path;
            tables.add(table);
            indexMap.put(path.getFileName().toString(), tableIndex);
            tableIndex++;
        }
        for (int i = 0; i < Constant.THREAD_COUNT; ++i) {
            rThreads[i] = new ReadThread();
            wThreads[i] = new WriteThread();
            rThreads[i].bq = wThreads[i].bq;
            rThreads[i].tId = i;
            rThreads[i].tables = tables;
        }
        for (ReadThread thread : rThreads) thread.start();
        for (WriteThread thread : wThreads) thread.start();
        for (WriteThread thread : wThreads) thread.join();
        System.out.println("COST TIME : " + (System.currentTimeMillis() - t));
        calTotalSize();
    }

    private void calTotalSize() {
        for (MyTable table : tables) {
            int cnt = 0;
            for (int j = 0; j < Constant.THREAD_COUNT; ++j) {
                for (int k = 0; k < Constant.PAGE_COUNT; ++k) {
                    cnt += table.pageCounts[j][k][0];
                }
            }
            table.dataCount = cnt;
            System.out.println("table: " + cnt);
        }
    }

    private int debug = 1;

    @Override
    public String quantile(String table, String column, double percentile) throws IOException {
        if (debug > 30) return "0";
        debug++;
        int tIdx = indexMap.get(table);
        int colIdx = tables.get(tIdx).colIndexMap.get(column);
        long ans = MyPageManager.find(tables.get(tIdx), tIdx, colIdx, percentile);
         System.out.println("query: " + table + ", column: " + column + ", percentile:" + percentile + ", ans:" + ans);
        return String.valueOf(ans);
    }

}
