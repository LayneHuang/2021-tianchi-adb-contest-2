package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.Constant;
import com.aliyun.adb.contest.cache.MyBlockingQueueCache;
import com.aliyun.adb.contest.page.MyBlock;
import com.aliyun.adb.contest.page.MyTable;
import com.aliyun.adb.contest.page.MyValuePage;
import sun.misc.Cleaner;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

public class ReadThread extends Thread {
    /**
     * 线程序号
     */
    private final int tId;

    private final List<MyTable> tables;

    private MyTable table;

    private final MyBlockingQueueCache bq;

    private final MyValuePage[] pages = new MyValuePage[Constant.MAX_COL_COUNT * Constant.PAGE_COUNT];

    private int blockCountInThread;

    public ReadThread(int tId, List<MyTable> tables, MyBlockingQueueCache bq) {
        this.tId = tId;
        this.tables = tables;
        this.bq = bq;
    }

    @Override
    public void run() {
        // 多表错位读, 减少同表的并发
        int tableSize = tables.size();
        for (int i = 0; i < tableSize; ++i) {
            table = tables.get((tId + i) % tableSize);
            initPages();
            readTable();
        }
        bq.put(new WriteTask());
    }

    private void initPages() {
        for (int cIdx = 0; cIdx < Constant.MAX_COL_COUNT; ++cIdx) {
            for (int pIdx = 0; pIdx < Constant.PAGE_COUNT; ++pIdx) {
                int key = getKey(cIdx, pIdx);
                if (pages[key] == null) {
                    pages[key] = new MyValuePage();
                } else {
                    pages[key].size = 0;
                    pages[key].dataCount = 0;
                }
            }
        }
    }

    private void readTable() {
        try (FileChannel channel = FileChannel.open(table.path)) {
            long fileSize = channel.size();
            // 分成多少块
            int DEFAULT_BLOCK_COUNT = (int) (fileSize / Constant.MAPPED_SIZE)
                    + (fileSize % Constant.MAPPED_SIZE == 0 ? 0 : 1);
            // 每个线程读多少块
            int BLOCK_PER_THREAD = (DEFAULT_BLOCK_COUNT / Constant.THREAD_COUNT)
                    + (DEFAULT_BLOCK_COUNT % Constant.THREAD_COUNT == 0 ? 0 : 1);

            int beginBIdx = tId * BLOCK_PER_THREAD;
            int endBIdx = Math.min(beginBIdx + BLOCK_PER_THREAD, DEFAULT_BLOCK_COUNT);

            if (beginBIdx >= DEFAULT_BLOCK_COUNT) {
                return;
            }
            table.initBlocks(DEFAULT_BLOCK_COUNT);
            System.out.println("DEFAULT_BLOCK_COUNT: " + DEFAULT_BLOCK_COUNT + ", BLOCK_PER_THREAD: " + BLOCK_PER_THREAD + ", beginBIdx: " + beginBIdx + ", endIndex: " + endBIdx);
            blockCountInThread = endBIdx - beginBIdx;

            for (int i = 0; i < blockCountInThread; ++i) {
                int bIdx = beginBIdx + i;
                long begin = Constant.MAPPED_SIZE * bIdx;
                long end = Math.min(begin + Constant.MAPPED_SIZE, fileSize);
                MappedByteBuffer buffer = channel.map(
                        FileChannel.MapMode.READ_ONLY,
                        begin,
                        end - begin
                );
                trans(i, table.blocks[bIdx], buffer);
                Cleaner cleaner = ((sun.nio.ch.DirectBuffer) buffer).cleaner();
                if (cleaner != null) {
                    cleaner.clean();
                }
            }
            finish();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void trans(int bIdxInThread, MyBlock block, MappedByteBuffer buffer) {
        initCur();
        byte b;
        if (tId == 0 && bIdxInThread == 0) {
            StringBuilder colName = new StringBuilder();
            while (buffer.hasRemaining()) {
                b = buffer.get();
                // 去除文件头的列名
                if (b == 10) {
                    String[] names = colName.toString().split(",");
                    for (int i = 0; i < names.length; ++i) {
                        table.colIndexMap.put(names[i], i);
                    }
                    break;
                } else {
                    if (b == 13) continue;
                    colName.append((char) b);
                }
            }
        } else {
            long num = 0;
            int firstLen = 0;
            while (buffer.hasRemaining()) {
                b = buffer.get();
                if (b >= 48 && b < 58) {
                    num = num * 10 + b - 48;
                    firstLen++;
                    continue;
                }
                if (block.beginLen == 0) block.firstNumLen = firstLen;
                block.begins[block.beginLen++] = num;
                num = 0;
                if (b == 10 || b == 13) {
                    break;
                }
            }
        }
        while (buffer.hasRemaining()) {
            b = buffer.get();
            handleByte(b);
        }
        setCurToBlock(block);
    }

    private long input;
    private int nowColIndex;
    private int maxDataLen;

    private void initCur() {
        input = 0;
        nowColIndex = 0;
        maxDataLen = 0;
    }

    private void handleByte(byte b) {
        if (b >= 48) {
            input = input * 10 + b - 48;
            maxDataLen++;
        } else if (b == 46) {
            // 小数点
            System.out.println("HAS DOUBLE!!!");
            maxDataLen = 0;
        } else {
            if (maxDataLen > 0) {
                putData(nowColIndex);
            }
            maxDataLen = 0;
            nowColIndex = (b == 44) ? (nowColIndex + 1) : 0;
            input = 0;
        }
    }

    private void putData(int cIdx) {
        if (input == 0) return;
        int pIdx = Constant.getPageIndex(input);
        int key = getKey(cIdx, pIdx);
        pages[key].add(input);
        if (pages[key].full()) {
            bq.put(new WriteTask(
                    pages[key].copy(),
                    Constant.getPath(tId, table.index, cIdx, pIdx),
                    table.index,
                    cIdx,
                    pIdx
            ));
            pages[key].size = 0;
        }
    }

    private void finish() {
        // 最后一块读完
        int readCount = table.readCount.addAndGet(blockCountInThread);
        if (readCount >= table.blockCount) {
            mergeBlocks();
            System.out.println("table: " + table.index + " read finished, " + readCount + ", " + table.blockCount);
        }
        flushRestPage();
    }

    /**
     * 记录每个 Block 最后一个值, 用来合并 Block
     */
    private void setCurToBlock(MyBlock block) {
        block.lastColIndex = nowColIndex;
        block.lastInput = input;
    }

    /**
     * 处理 Blocks 的边缘值
     */
    private void mergeBlocks() {
        for (int i = 0; i < table.blockCount - 1; ++i) {
            MyBlock block = table.blocks[i];
            MyBlock nxtBlock = table.blocks[i + 1];
            input = block.lastInput * (long) Math.pow(10, nxtBlock.firstNumLen) + nxtBlock.begins[0];
            putData(block.lastColIndex);
            for (int j = 1; j < nxtBlock.beginLen; ++j) {
                input = nxtBlock.begins[j];
                putData(block.lastColIndex + j);
            }
        }
    }

    /**
     * 剩下的 Page 落盘
     */
    private void flushRestPage() {
        for (int cIdx = 0; cIdx < Constant.MAX_COL_COUNT; ++cIdx) {
            for (int pIdx = 0; pIdx < Constant.PAGE_COUNT; ++pIdx) {
                int key = getKey(cIdx, pIdx);
                table.pageCounts[tId][pIdx][cIdx] += pages[key].dataCount;
                if (pages[key].size == 0) continue;
                bq.put(new WriteTask(
                        pages[key].copy(),
                        Constant.getPath(tId, table.index, cIdx, pIdx),
                        table.index,
                        cIdx,
                        pIdx
                ));
            }
        }
    }

    private int getKey(int colIndex, int pageIndex) {
        return colIndex * Constant.PAGE_COUNT + pageIndex;
    }
}
