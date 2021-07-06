package com.aliyun.adb.contest.page;

import com.aliyun.adb.contest.Constant;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/**
 * @Classname Constant
 * @Description
 * @Date 2021/5/25 21:08
 * @Created by FinkyS
 */
public final class MyFilePage extends MyPage {

    public static Path WORK_DIR;

    public Path path;

    public FileChannel fileChannel;

    public ByteBuffer byteBuffer;

    @Override
    public void add(long value) {
        byteBuffer.putLong(value);
        size++;
        if (!byteBuffer.hasRemaining()) {
            try {
                byteBuffer.flip();
                fileChannel.write(byteBuffer);
                byteBuffer.clear();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void setMinValue(long value) {
    }

    @Override
    public long find(int index) {
        try {
            if (fileChannel.isOpen()) {
                close();
            }
            long[] data = new long[size];
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 64 * 1024);
            int i = 0;
            while (channel.read(buffer) > 0) {
                buffer.flip();
                while (buffer.hasRemaining()) {
                    data[i] = buffer.getLong();
                    i++;
                }
                buffer.clear();
            }
            Arrays.parallelSort(data);
            return data[index];
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public long[] getValues() {
        long[] values = new long[size];
        try {
            if (fileChannel.isOpen()) {
                close();
            }
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
            ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
            int i = 0;
            while (channel.read(buffer) > 0) {
                buffer.flip();
                while (buffer.hasRemaining()) {
                    values[i] = buffer.getLong();
                    i++;
                }
                buffer.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return values;
    }

    public void close() {
        try {
            if (byteBuffer.position() != 0) {
                byteBuffer.flip();
                fileChannel.write(byteBuffer);
                byteBuffer.clear();
            }
            fileChannel.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public MyFilePage(long startValue, long endValue, int threadIndex) {
        this.startValue = startValue;
        this.endValue = endValue;
        this.path = WORK_DIR.resolve(threadIndex + "_" + startValue);
        try {
            fileChannel = FileChannel.open(path,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );
            byteBuffer = ByteBuffer.allocate(Constant.WRITE_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
