package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.Constant;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class ReaderPool {
    private final BlockingQueue<MappedByteBuffer> bq = new LinkedBlockingDeque<>(2);

    private final ExecutorService executor = Executors.newFixedThreadPool(Constant.THREAD_COUNT);

    public void readFile(Path path) {
        long fileSize = getFileSize(path);
        int blockCount = (int) (fileSize / Constant.MAPPED_SIZE)
                + (fileSize % Constant.MAPPED_SIZE == 0 ? 0 : 1);
        for (int i = 0; i < blockCount; i++) {
            final long begin = (long) i * Constant.MAPPED_SIZE;
            final long end = Math.min(fileSize - 1, begin + Constant.MAPPED_SIZE);
            executor.execute(() -> readFileBlock(path, begin, end));
        }
    }

    public long getFileSize(Path path) {
        try (FileChannel channel = FileChannel.open(path)) {
            return channel.size();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public void readFileBlock(Path path, long begin, long end) {
        try (FileChannel channel = FileChannel.open(path)) {
            MappedByteBuffer buffer = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    begin,
                    end - begin + 1
            );

            bq.put(buffer.load());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public MappedByteBuffer take() throws InterruptedException {
        return bq.take();
    }
}