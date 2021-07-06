package com.aliyun.adb.contest;

import com.aliyun.adb.contest.page.MyFilePage;
import com.aliyun.adb.contest.page.MyTable;
import com.aliyun.adb.contest.pool.ReaderPool;
import com.aliyun.adb.contest.pool.WritePool;
import com.aliyun.adb.contest.spi.AnalyticDB;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class SimpleAnalyticDB implements AnalyticDB {
    private ReaderPool readerPool = new ReaderPool();
    private WritePool writePool = new WritePool();

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
        int tableIndex = 0;
        for (Path path : tablePaths) {
            if (path.getFileName().toString().equals("results")) {
                // 跳过结果数据
                return;
            }
            MyTable table = readerPool.start(tableIndex, path, writePool);
            tableIndex++;
        }
        System.out.println("COST TIME : " + (System.currentTimeMillis() - t));
    }

    private int quantileCount = 0;

    @Override
    public String quantile(String table, String column, double percentile) {
        if (quantileCount++ > 8) {
            return "";
        }
        return "0";
    }

}
