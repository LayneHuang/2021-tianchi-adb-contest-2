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
        long rank = Math.round(table.dataCount * percentile) - 1;
        ByteBuffer buffer = ByteBuffer.allocate(Constant.WRITE_SIZE);
        long offset = 0;
        for (int pIdx = 0; pIdx < Constant.PAGE_COUNT; ++pIdx) {
            int pageSize = getPageSize(table, cIdx, pIdx);
            if (rank <= offset + pageSize) {
                System.out.println("FOUND!!, block count:" + table.blockCount);
                long[] data = new long[pageSize];
                int index = 0;
                for (int bIdx = 0; bIdx < table.blockCount; bIdx++) {
                    if( table.pageCounts[bIdx][pIdx][cIdx] == 0 ) continue;
                    Path path = Constant.getPath(tIdx, cIdx, bIdx, pIdx);
                    System.out.println("FOUND!!, tIdx: " + tIdx + " cIdx: " + cIdx + " pIdx: " + pIdx + ", pageSize: " + pageSize);
                    FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
                    while (channel.read(buffer) > 0) {
                        buffer.flip();
                        while (buffer.hasRemaining()) {
                            long d = buffer.getLong();
                            System.out.println(d);
                            data[index++] = d;
                        }
                        buffer.clear();
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
//            System.out.println(bIdx + " " + table.pageCounts[bIdx][pIdx][cIdx]);
        }
        return pageSize;
    }
}
