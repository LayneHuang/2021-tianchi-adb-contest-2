package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.Constant;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

public class WriteThread extends Thread {

    public MyBlockingQueue bq = new MyBlockingQueue();

    private final Set<String> st = new HashSet<>();

    private final ByteBuffer buffer = ByteBuffer.allocate(Constant.WRITE_SIZE);

    public WriteThread() {
    }

    @Override
    public void run() {
        while (true) {
            WriteTask task = bq.poll();
            if (task == null || task.getPath() == null) {
                break;
            }
            write(task);
        }
    }

    private void write(WriteTask task) {
        try (FileChannel fileChannel = FileChannel.open(
                task.getPath(),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                st.contains(task.getPath().toString()) ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING
        )) {
            st.add(task.getPath().toString());
            trans(task.getData());
            buffer.flip();
            fileChannel.write(buffer);
            buffer.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void trans(long[] data) {
        buffer.clear();
        for (long d : data) buffer.putLong(d);
    }
}
