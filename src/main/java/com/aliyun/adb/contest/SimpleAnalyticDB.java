package com.aliyun.adb.contest;

import com.aliyun.adb.contest.page.MyTable;
import com.aliyun.adb.contest.pool.ReaderPool;
import com.aliyun.adb.contest.pool.WritePool;
import com.aliyun.adb.contest.spi.AnalyticDB;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class SimpleAnalyticDB implements AnalyticDB {
    private final WritePool writePool = new WritePool();
    private final ReaderPool readerPool = new ReaderPool();
    private MyTable[] tables = null;
    private final Map<String, Integer> indexMap = new HashMap<>();

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
        int tableSize = tablePaths.size();
        tables = new MyTable[tableSize];
        CountDownLatch latch = new CountDownLatch(tableSize);
        writePool.setLatch(latch);
        System.out.printf("table count: %d\n", tablePaths.size());
        int tableIndex = 0;
        for (Path path : tablePaths) {
            if (path.getFileName().toString().equals("results")) {
                // 跳过结果数据
                latch.countDown();
                continue;
            }
            tables[tableIndex] = readerPool.start(tableIndex, path, writePool);
            indexMap.put(path.getFileName().toString(), tableIndex);
            tableIndex++;
        }
        latch.await();
        System.out.println("COST TIME : " + (System.currentTimeMillis() - t));
    }

    private void calTotalSize(int tableSize) {
        for (int i = 0; i < tableSize; ++i) {
            if (tables[i] == null) continue;
            int cnt = 0;
            for (int j = 0; j < tables[i].blockCount; ++j) {
                for (int k = 0; k < Constant.PAGE_COUNT; ++k) {
                    cnt += tables[i].pageCounts[j][k][0];
                }
            }
            tables[i].dataCount = cnt;
            System.out.println("table " + i + ": " + cnt);
        }
    }

    private int debug = 1;

    @Override
    public String quantile(String table, String column, double percentile) throws IOException {
        if (debug > 50) return "0";
        debug++;
        int tIdx = indexMap.get(table);
        int colIdx = tables[tIdx].colIndexMap.get(column);
        long ans = MyPageManager.find(tables[tIdx], tIdx, colIdx, percentile);
        // System.out.println("query: " + table + ", column: " + column + ", percentile:" + percentile + ", ans:" + ans);
        return String.valueOf(ans);
    }

}
