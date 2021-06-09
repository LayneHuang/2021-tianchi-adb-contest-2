package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class TestSimpleAnalyticDB {

    public static void main(String[] args) throws Exception {
        AnalyticDB analyticDB = new SimpleAnalyticDB();
        String rootDir = System.getProperty("user.dir");
        String tpchDataFileDir = rootDir + File.separator + "test_data";
        String workspaceDir = rootDir + File.separator + "test_workspace";
        analyticDB.load(tpchDataFileDir, workspaceDir);

        Path resultsFile = Paths.get(tpchDataFileDir + File.separator + "results");
        List<String> lines = Files.readAllLines(resultsFile);
        for (String line : lines) {
            String[] split = line.split(" ");
            String quantile = analyticDB.quantile(split[0], split[1], Double.parseDouble(split[2]));
            if (!split[3].equals(quantile)) {
                throw new Exception("答案错误 " + line + " , 你的答案 " + quantile);
            }
        }
    }
}
