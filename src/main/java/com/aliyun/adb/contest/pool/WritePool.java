package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.Constant;
import com.aliyun.adb.contest.page.MyTable;

import java.io.IOException;
import java.lang.reflect.WildcardType;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WritePool {
    private final ExecutorService executor = Executors.newFixedThreadPool(Constant.THREAD_COUNT);

    private CountDownLatch latch;

    public WritePool(CountDownLatch latch) {
        this.latch = latch;
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public void execute(MyTable table, Path path, ByteBuffer buffer) {
        table.addPageCount();
        executor.execute(() -> handleBlock(table, path, buffer));
    }

    private void handleBlock(MyTable table, Path path, ByteBuffer buffer) {
        buffer.flip();
        try (FileChannel fileChannel = FileChannel.open(path,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            fileChannel.write(buffer);
            table.addWriteCount();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (table.finished()) {
            latch.countDown();
        }
    }
}
