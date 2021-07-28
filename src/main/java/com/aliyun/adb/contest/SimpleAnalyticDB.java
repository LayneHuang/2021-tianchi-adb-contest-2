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
    private TableInfoPersistence tableInfoDB = new TableInfoPersistence();

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
            return;
        }
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
            table.name = path.getFileName().toString();
            tables.add(table);
            indexMap.put(table.name, tableIndex);
            tableIndex++;
        }
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

    @Override
    public String quantile(String table, String column, double percentile) throws IOException {
        int tIdx = indexMap.get(table);
        int colIdx = tables.get(tIdx).colIndexMap.get(column);
        System.out.println("tIdx: " + tIdx + ", cIdx: " + colIdx);
        long ans = MyPageManager.find(tables.get(tIdx), tIdx, colIdx, percentile);
        System.out.println("query: " + table + ", column: " + column + ", percentile:" + percentile + ", ans:" + ans);
        return String.valueOf(ans);
    }

}
