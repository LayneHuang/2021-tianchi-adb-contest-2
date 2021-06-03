package com.aliyun.adb.contest;


import com.aliyun.adb.contest.cache.MyBufferCache;
import com.aliyun.adb.contest.cache.MyCache;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class MyFileReader extends Thread {

    public Path path;
    public long bufferSize;
    public int threadIndex;
    public int threadCount;
    public int pageCount;
    public boolean notLastThread;
    public long currentBlockCount;
    public long totalStart;
    public MyCache myCache;

    private FileChannel channel;
    private long fileSize;

    public MyFileReader(Path path, int threadIndex, int threadCount, long bufferSize, int pageCount) {
        this.path = path;
        this.threadIndex = threadIndex;
        this.threadCount = threadCount;
        this.bufferSize = bufferSize;
        this.pageCount = pageCount;
        this.notLastThread = threadIndex != threadCount - 1;
        myCache = new MyBufferCache();
        try {
            channel = FileChannel.open(path);
            fileSize = channel.size();
            calculateBlockCount(fileSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void calculateBlockCount(long fileSize){
        long totalBlockCount = fileSize / bufferSize;
        if (fileSize % bufferSize != 0) {
            totalBlockCount++;
        }
        currentBlockCount = totalBlockCount / threadCount;
        totalStart = bufferSize * currentBlockCount * threadIndex;
        if (!notLastThread && totalBlockCount % threadCount != 0) {
            currentBlockCount++;
        }
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < currentBlockCount; i++) {
                long start = totalStart + i * bufferSize;
                long end = totalStart + (i + 1) * bufferSize - 1;
                if (!notLastThread && i == currentBlockCount - 1) {
                    end = fileSize - 1;
                }
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, start, end - start + 1);
                myCache.put(buffer);
            }
            channel.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
