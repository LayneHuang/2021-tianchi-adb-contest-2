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
        long rank = Math.round(Constant.LINE_COUNT * percentile) - 1;
        long offset = 0;
        for (int pIdx = 0; pIdx < Constant.PAGE_COUNT; ++pIdx) {
            int pageSize = 0;
            for (int bIdx = 0; bIdx < table.blockCount; bIdx++) {
                pageSize += table.pageCounts[bIdx][pIdx];
            }
            if (rank <= offset + pageSize) {
                System.out.println("FOUND!!");
                long[] data = new long[pageSize];
                for (int bIdx = 0; bIdx < table.blockCount; bIdx++) {
                    Path path = Constant.getPath(tIdx, cIdx, bIdx, pIdx);
                    ByteBuffer buffer = ByteBuffer.allocate(table.pageCounts[bIdx][pIdx] * Long.BYTES);
                    FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
                    int index = 0;
                    while (channel.read(buffer) > 0) {
                        buffer.flip();
                        while (buffer.hasRemaining()) {
                            data[index] = buffer.getLong();
                            System.out.println(data[index]);
                            index++;
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
}
