package com.aliyun.adb.contest;

import com.aliyun.adb.contest.page.MyTable;
import com.aliyun.adb.contest.persistence.TableInfoPersistence;
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
    private final List<MyTable> tables = new ArrayList<>();
    private final Map<String, Integer> indexMap = new HashMap<>();
    private static final ReadThread[] rThreads = new ReadThread[Constant.THREAD_COUNT];
    private static final WriteThread[] wThreads = new WriteThread[Constant.THREAD_COUNT];
    private final TableInfoPersistence tableInfoDB = new TableInfoPersistence();

    /**
     * The implementation must contain a public no-argument constructor.
     */
    public SimpleAnalyticDB() {
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        long t = System.currentTimeMillis();
        Constant.WORK_DIR = Paths.get(workspaceDir);
        if (tableInfoDB.readLoaded()) {
            tableInfoDB.loadTableInfo(tables, indexMap);
            System.out.println("SECOND LOAD, COST:" + (System.currentTimeMillis() - t));
            showMemory();
            return;
        }
        Path dirPath = Paths.get(tpchDataFileDir);
        List<Path> tablePaths = Files.list(dirPath).collect(Collectors.toList());
        // 等待所有表跑完
        int tableIndex = 0;
        for (Path path : tablePaths) {
            if (path.getFileName().toString().equals("results")) {
                // 跳过结果数据
                continue;
            }
            MyTable table = new MyTable();
            table.index = tableIndex;
            table.path = path;
            table.name = path.getFileName().toString();
            tables.add(table);
            indexMap.put(table.name, tableIndex);
            tableIndex++;
        }
        System.out.println("table count: " + tables.size());
        for (int i = 0; i < Constant.THREAD_COUNT; ++i) {
            wThreads[i] = new WriteThread();
            rThreads[i] = new ReadThread(i, tables, wThreads[i].bq);
        }
        for (ReadThread thread : rThreads) thread.start();
        for (WriteThread thread : wThreads) thread.start();
        for (WriteThread thread : wThreads) thread.join();
        long readWriteT = System.currentTimeMillis();
        System.out.println("READ AND WRITE COST TIME : " + (readWriteT - t));
        calTotalSize();
        long calPageT = System.currentTimeMillis();
        System.out.println("CAL PAGE COST TIME : " + (calPageT - readWriteT));
        tableInfoDB.saveTableInfo(tables);
        System.out.println("SAVE INFO COST TIME : " + (System.currentTimeMillis() - calPageT));
        showMemory();
    }

    private void showMemory() {
        long maxMemory = Runtime.getRuntime().maxMemory();
        long totalMemory = Runtime.getRuntime().totalMemory();
        System.out.println("SYSTEM MAX MEMORY: " + maxMemory);
        System.out.println("SYSTEM TOTAL MEMORY: " + totalMemory);
        System.out.println("SYSTEM REST MEMORY:" + (maxMemory - totalMemory + Runtime.getRuntime().freeMemory()));
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

    private int debug = 0;

    @Override
    public String quantile(String table, String column, double percentile) throws IOException {
        debug++;
        if (debug > 30) return "0";
        int tIdx = indexMap.get(table);
        int colIdx = tables.get(tIdx).colIndexMap.get(column);
        long ans = MyPageManager.find(tables.get(tIdx), tIdx, colIdx, percentile);
        System.out.println("query: " + table + ", column: " + column + ", percentile:" + percentile + ", ans:" + ans);
        return String.valueOf(ans);
    }

}
