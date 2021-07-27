package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.cache.MyBlockingQueueCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

public class WriteThread extends Thread {

    public MyBlockingQueueCache bq = new MyBlockingQueueCache();

    private Set<String> st = new HashSet<>();

    public WriteThread() {
    }

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
                    st.contains(task.getPath().toString()) ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING
            )) {
                st.add(task.getPath().toString());
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
