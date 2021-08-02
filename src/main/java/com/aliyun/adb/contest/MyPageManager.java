package com.aliyun.adb.contest;

import com.aliyun.adb.contest.cache.DataCache;
import com.aliyun.adb.contest.page.MyTable;
import sun.misc.Cleaner;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public final class MyPageManager {

    private static final DataCache cache = new DataCache();

    public static long find(MyTable table, int tableIdx, int cIdx, double percentile) throws IOException {
        long rank = Math.round(table.dataCount * percentile) - 1;
        if (rank < 0) rank = 0;
        // System.out.println("percentile: " + percentile + ", rank: " + rank);
        long offset = 0;
        for (int pIdx = 0; pIdx < Constant.PAGE_COUNT; ++pIdx) {
            int pageSize = getPageSize(table, cIdx, pIdx);
            if (rank <= offset + pageSize) {
                // System.out.println("Found in Page: " + pIdx + ", PageSize: " + pageSize);
                long[] data = cache.query(tableIdx, cIdx, pIdx);
                if (data == null) {
                    data = readFromFile(table, pageSize, tableIdx, cIdx, pIdx);
                    cache.cache(tableIdx, cIdx, pIdx, data);
                }
                return data[(int) (rank - offset)];
            }
            offset += pageSize;
        }
        return -1;
    }

    private static long[] readFromFile(MyTable table,
                                       int pageSize,
                                       int tIdx,
                                       int cIdx,
                                       int pIdx) throws IOException {
        long[] data = new long[pageSize];
        int index = 0;
        for (int threadIdx = 0; threadIdx < Constant.THREAD_COUNT; ++threadIdx) {
            if (table.pageCounts[threadIdx][pIdx][cIdx] == 0) continue;
            Path path = Constant.getPath(threadIdx, tIdx, cIdx, pIdx);
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                MappedByteBuffer buffer = channel.map(
                        FileChannel.MapMode.READ_ONLY,
                        0,
                        (long) table.pageCounts[threadIdx][pIdx][cIdx] * Long.BYTES
                );
                while (buffer.hasRemaining()) {
                    long d = buffer.getLong();
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
//        System.out.println("page size:" + pageSize + ", index size:" + index);
        return data;
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
