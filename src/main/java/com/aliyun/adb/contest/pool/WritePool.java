package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.Constant;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WritePool {
    private final ExecutorService executor = Executors.newFixedThreadPool(Constant.THREAD_COUNT);

    public void execute(Path path, ByteBuffer buffer) {
        executor.execute(() -> handleBlock(path, buffer));
    }

    private void handleBlock(Path path, ByteBuffer buffer) {
        buffer.flip();
        try (FileChannel fileChannel = FileChannel.open(path,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            fileChannel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
