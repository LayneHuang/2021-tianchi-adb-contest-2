package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.Constant;
import com.aliyun.adb.contest.page.MyTable;

import java.io.IOException;
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

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public void execute(MyTable table, Path path, ByteBuffer buffer) {
        if (buffer == null || buffer.position() == 0) {
            checkFinished(table);
            return;
        }
        executor.execute(() -> handleBlock(table, path, buffer));
    }

    private void handleBlock(MyTable table, Path path, ByteBuffer buffer) {
        try (FileChannel fileChannel = FileChannel.open(path,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
//            showBuffer(buffer);
            buffer.flip();
            fileChannel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        checkFinished(table);
    }

    private void checkFinished(MyTable table) {
        int writeCount = table.writeCount.incrementAndGet();
        if (table.finished()) {
            System.out.println("table " + table.index + " write finished");
            latch.countDown();
        }
    }

    private void showBuffer(ByteBuffer buffer) {
        buffer.flip();
        while (buffer.hasRemaining()) {
            long v = buffer.getLong();
            System.out.println(v);
        }
    }
}
