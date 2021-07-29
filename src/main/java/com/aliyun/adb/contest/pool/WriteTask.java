package com.aliyun.adb.contest.pool;

import java.nio.file.Path;

public class WriteTask {
    private long[] data;

    private Path path;

    public WriteTask() {
    }

    public WriteTask(long[] data, Path path) {
        this.data = data;
        this.path = path;
    }

    public long[] getData() {
        return data;
    }

    public Path getPath() {
        return path;
    }
}
