package com.aliyun.adb.contest.pool;

import java.nio.file.Path;

public class WriteTask {

    private int tableId;

    private int cIdx;

    private int pIdx;

    private long[] data;

    private Path path;

    public WriteTask() {
    }

    public WriteTask(long[] data, Path path, int tableId, int cIdx, int pIdx) {
        this.data = data;
        this.path = path;
        this.tableId = tableId;
        this.cIdx = cIdx;
        this.pIdx = pIdx;
    }

    public long[] getData() {
        return data;
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
