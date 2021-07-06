package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.Constant;
import com.aliyun.adb.contest.page.MyTable;
import com.aliyun.adb.contest.page.MyValuePage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WritePool {
    private final ExecutorService executor = Executors.newFixedThreadPool(Constant.THREAD_COUNT);

    private final BlockingQueue<MyValuePage> bq;

    WritePool(BlockingQueue<MyValuePage> bq) {
        this.bq = bq;
    }

    public void start(MyTable table) {
        for (int i = 0; i < table.blockCount; ++i) {
            executor.execute(this::handleBlock);
        }
    }

    public void handleBlock() {
        try {
            MyValuePage page = bq.take();
            page.byteBuffer.flip();
            Path path = Constant.getPath(page);
            try (FileChannel fileChannel = FileChannel.open(path,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            )) {
                fileChannel.write(page.byteBuffer);
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
