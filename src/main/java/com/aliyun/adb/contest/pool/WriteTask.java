package com.aliyun.adb.contest.pool;

import java.nio.ByteBuffer;
import java.nio.file.Path;

public class WriteTask {

    private ByteBuffer buffer;

    private Path path;

    public WriteTask() {
    }

    public WriteTask(ByteBuffer buffer, Path path) {
        this.buffer = buffer;
        this.path = path;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public Path getPath() {
        return path;
    }
}
