package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.Constant;
import com.aliyun.adb.contest.page.MyBlock;
import com.aliyun.adb.contest.page.MyTable;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class ReaderPool {
    private final BlockingQueue<MyBlock> bq = new LinkedBlockingDeque<>(16);

    private final ExecutorService executor = Executors.newFixedThreadPool(Constant.THREAD_COUNT);

    public int readBlockCount = 0;

    public MyTable start(final int tableIndex, Path path) {
        long fileSize = getFileSize(path);
        int blockCount = (int) (fileSize / Constant.MAPPED_SIZE)
                + (fileSize % Constant.MAPPED_SIZE == 0 ? 0 : 1);
        MyTable table = new MyTable(blockCount);
        readBlockCount += blockCount;
        for (int i = 0; i < blockCount; i++) {
            MyBlock block = new MyBlock();
            block.tableIndex = tableIndex;
            block.blockIndex = i;
            block.begin = (long) i * Constant.MAPPED_SIZE;
            block.end = Math.min(fileSize - 1, block.begin + Constant.MAPPED_SIZE);
            table.blocks[i] = block;
            executor.execute(() -> readFileBlock(path, block));
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

    public void readFileBlock(Path path, MyBlock block) {
        try (FileChannel channel = FileChannel.open(path)) {
            MappedByteBuffer buffer = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    block.begin,
                    block.getSize()
            );
            block.trans(buffer.load());
            bq.put(block);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public MyBlock take() throws InterruptedException {
        return bq.take();
    }
}