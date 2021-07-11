package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.Constant;
import com.aliyun.adb.contest.page.MyBlock;
import com.aliyun.adb.contest.page.MyTable;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReaderPool {

    private final ExecutorService executor = Executors.newFixedThreadPool(Constant.THREAD_COUNT);

    public int readBlockCount = 0;

    public MyTable start(int tableIndex, Path path, WritePool writePool) {
        long fileSize = getFileSize(path);
        int blockCount = (int) (fileSize / Constant.MAPPED_SIZE)
                + (fileSize % Constant.MAPPED_SIZE == 0 ? 0 : 1);
        MyTable table = new MyTable(blockCount);
        table.index = tableIndex;
        readBlockCount += blockCount;
        table.pageCounts = new int[2][blockCount][Constant.PAGE_COUNT];
        for (int i = 0; i < blockCount; i++) {
            MyBlock block = new MyBlock();
            block.tableIndex = tableIndex;
            block.blockIndex = i;
            block.begin = (long) i * Constant.MAPPED_SIZE;
            block.end = Math.min(fileSize - 1, block.begin + Constant.MAPPED_SIZE);
            table.blocks[i] = block;
            ReadTask task = new ReadTask(path, table, block, writePool);
            executor.execute(task);
        }
        return table;
    }

    public long getFileSize(Path path) {
        try (FileChannel channel = FileChannel.open(path)) {
            return channel.size();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}