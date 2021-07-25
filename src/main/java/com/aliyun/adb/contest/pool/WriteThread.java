package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.cache.MyBlockingQueueCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class WriteThread extends Thread {

    public MyBlockingQueueCache bq = new MyBlockingQueueCache();

    @Override
    public void run() {
        while (true) {
            WriteTask task = bq.poll();
            if (task == null || task.getBuffer() == null) {
                break;
            }
            try (FileChannel fileChannel = FileChannel.open(
                    task.getPath(),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            )) {
                ByteBuffer buffer = task.getBuffer();
                buffer.flip();
                fileChannel.write(buffer);
                buffer.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
