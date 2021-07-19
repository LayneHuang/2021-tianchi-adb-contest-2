package com.aliyun.adb.contest;

import com.aliyun.adb.contest.page.MyTable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public final class MyPageManager {

    public static long find(MyTable table, int tIdx, int cIdx, double percentile) throws IOException {
        long rank = Math.round(Constant.DATA_SIZE * percentile) - 1;
        if (rank < 0) rank = 0;
//        System.out.println("percentile: " + percentile + ", rank: " + rank);
        long offset = 0;
        for (int pIdx = 0; pIdx < Constant.PAGE_COUNT; ++pIdx) {
            int pageSize = getPageSize(table, cIdx, pIdx);
            if (rank <= offset + pageSize) {
//                System.out.println("Found in Page: " + pIdx);
                long[] data = new long[pageSize];
                int index = 0;
                for (int bIdx = 0; bIdx < table.blockCount; bIdx++) {
                    if (table.pageCounts[bIdx][pIdx][cIdx] == 0) continue;
                    Path path = Constant.getPath(tIdx, cIdx, bIdx, pIdx);
                    ByteBuffer buffer = ByteBuffer.allocate(table.pageCounts[bIdx][pIdx][cIdx] * Long.BYTES);
                    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                        while (channel.read(buffer) > 0) {
                            buffer.flip();
                            while (buffer.hasRemaining()) {
                                long d = buffer.getLong();
                                data[index++] = d;
                            }
                            buffer.clear();
                        }
                    }
                }
                Arrays.parallelSort(data);
                return data[(int) (rank - offset)];
            }
            offset += pageSize;
        }
        return -1;
    }

    private static int getPageSize(MyTable table, int cIdx, int pIdx) {
        int pageSize = 0;
        for (int bIdx = 0; bIdx < table.blockCount; bIdx++) {
            pageSize += table.pageCounts[bIdx][pIdx][cIdx];
        }
        return pageSize;
    }
}
