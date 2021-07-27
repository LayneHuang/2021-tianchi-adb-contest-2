package com.aliyun.adb.contest.pool;

import java.nio.ByteBuffer;
import java.nio.file.Path;

public class WriteTask {

    private int tableId;

    private int cIdx;

    private int pIdx;

    private ByteBuffer buffer;

    private Path path;

    public WriteTask() {
    }

    public WriteTask(ByteBuffer buffer, Path path, int tableId, int cIdx, int pIdx) {
        this.buffer = buffer;
        this.path = path;
        this.tableId = tableId;
        this.cIdx = cIdx;
        this.pIdx = pIdx;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public Path getPath() {
        return path;
    }

    public int getTableId() {
        return tableId;
    }

    public int getCIdx() {
        return cIdx;
    }

    public int getPIdx() {
        return pIdx;
    }
}
