package com.aliyun.adb.contest;

import com.aliyun.adb.contest.page.MyTable;
import sun.misc.Cleaner;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public final class MyPageManager {

    public static long find(MyTable table, int tIdx, int cIdx, double percentile) throws IOException {
        long rank = Math.round(table.dataCount * percentile) - 1;
        if (rank < 0) rank = 0;
        // System.out.println("percentile: " + percentile + ", rank: " + rank);
        long offset = 0;
        for (int pIdx = 0; pIdx < Constant.PAGE_COUNT; ++pIdx) {
            int pageSize = getPageSize(table, cIdx, pIdx);
            if (rank <= offset + pageSize) {
                // System.out.println("Found in Page: " + pIdx + ", PageSize: " + pageSize);
                long[] data = new long[pageSize];
                int index = 0;
                long fileSize = 0;
                for (int threadIdx = 0; threadIdx < Constant.THREAD_COUNT; threadIdx++) {
                    if (table.pageCounts[threadIdx][pIdx][cIdx] == 0) continue;
                    Path path = Constant.getPath(tIdx, cIdx, threadIdx, pIdx);
                    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                        MappedByteBuffer buffer = channel.map(
                                FileChannel.MapMode.READ_ONLY,
                                0,
                                (long) table.pageCounts[threadIdx][pIdx][cIdx] * Long.BYTES
                        );
                        fileSize += channel.size() / 8;
                        while (buffer.hasRemaining()) {
                            long d = buffer.getLong();
                            if (index >= pageSize) {
                                System.out.println("FUCK");
                                break;
                            }
                            data[index++] = d;
                        }
                        buffer.clear();
                        Cleaner cleaner = ((sun.nio.ch.DirectBuffer) buffer).cleaner();
                        if (cleaner != null) {
                            cleaner.clean();
                        }
                    }
                }
                Arrays.parallelSort(data);
                // showData(data);
                System.out.println("page size:" + pageSize + ", index size:" + index + ", fileSize:" + fileSize);
                return data[(int) (rank - offset)];
            }
            offset += pageSize;
        }
        return -1;
    }

    private static int getPageSize(MyTable table, int cIdx, int pIdx) {
        int pageSize = 0;
        for (int threadIdx = 0; threadIdx < Constant.THREAD_COUNT; threadIdx++) {
            pageSize += table.pageCounts[threadIdx][pIdx][cIdx];
        }
        return pageSize;
    }

    private static void showData(long[] data) {
        int cnt = 0;
        for (long d : data) {
            cnt++;
            if (cnt > 20) break;
            System.out.print(d + " ");
        }
        System.out.println();
    }
}
