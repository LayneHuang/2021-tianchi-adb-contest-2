package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.Constant;
import com.aliyun.adb.contest.page.MyBlock;
import com.aliyun.adb.contest.page.MyPage;
import com.aliyun.adb.contest.page.MyTable;
import com.aliyun.adb.contest.page.MyValuePage;
import sun.misc.Cleaner;

import java.nio.ByteBuffer;
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

    private final MyBlockingQueue bq;

    private final MyPage[] pages = new MyValuePage[Constant.MAX_COL_COUNT * Constant.PAGE_COUNT];

    private int blockCountInThread;

    public ReadThread(int tId, List<MyTable> tables, MyBlockingQueue bq) {
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
        if (bq != null) {
            bq.put(new WriteTask());
        }
    }

    private void initPages() {
        for (int cIdx = 0; cIdx < Constant.MAX_COL_COUNT; ++cIdx) {
            for (int pIdx = 0; pIdx < Constant.PAGE_COUNT; ++pIdx) {
                int key = getKey(cIdx, pIdx);
                if (pages[key] == null) {
                    pages[key] = new MyValuePage();
                } else {
                    pages[key].clean();
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
//            System.out.println("DEFAULT_BLOCK_COUNT: " + DEFAULT_BLOCK_COUNT + ", BLOCK_PER_THREAD: " + BLOCK_PER_THREAD + ", beginBIdx: " + beginBIdx + ", endIndex: " + endBIdx);
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
                // 28S
                // noTrans(buffer);
                // 62S
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
//        System.out.println("transBeginT: " + (transBeginT / 1000000)
//                + "(ms), transT: " + (transT / 1000000)
//                + "(ms), divPageT: " + (divPageT / 1000000)
//                + "(ms), submitT: " + (submitT / 1000000) + "(ms)");
    }

    private void noTrans(MappedByteBuffer buffer) {
        long sum = 0;
        while (buffer.hasRemaining()) {
            sum += sum * 10 + (buffer.get() - 48);
        }
        System.out.println("sum: " + sum);
    }

//    private long transBeginT = 0;
//    /**
//     * 转换耗时
//     */
//    private long transT = 0;
//    /**
//     * 分页耗时
//     */
//    private long divPageT = 0;
//    /**
//     * 提交耗时
//     */
//    private long submitT = 0;

    private void trans(int bIdxInThread, MyBlock block, MappedByteBuffer buffer) {
        initCur();
//        long t1 = System.nanoTime();
        if (tId == 0 && bIdxInThread == 0) {
            handleColName(buffer);
        } else {
            handleBeginBytes(block, buffer);
        }
//        long t2 = System.nanoTime();
//        transBeginT += t2 - t1;
        while (buffer.hasRemaining()) {
            handleByte(buffer.get());
        }
        setCurToBlock(block);
//        long t3 = System.nanoTime();
//        transT += t3 - t2;
    }

    /**
     * 处理列名
     */
    private void handleColName(ByteBuffer buffer) {
        StringBuilder colName = new StringBuilder();
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
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
    }

    /**
     * 处理块开头不完整部分
     */
    private void handleBeginBytes(MyBlock block, ByteBuffer buffer) {
        long num = 0;
        int firstLen = 0;
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
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

    private long input;
    private int nowColIndex;

    private void initCur() {
        input = 0;
        nowColIndex = 0;
    }

    private void handleByte(byte b) {
        if (b >= 48) {
            input = input * 10 + b - 48;
        } else if (b == 46) {
            // 小数点
            System.out.println("HAS DOUBLE!!!");
        } else {
            putData(nowColIndex);
            nowColIndex = (b == 44) ? (nowColIndex + 1) : 0;
            input = 0;
        }
    }

    private void putData(int cIdx) {
        if (input == 0) return;
        int pIdx = Constant.getPageIndex(input);
        int key = getKey(cIdx, pIdx);
//        long t1 = System.nanoTime();
        pages[key].add(input);
//        long t2 = System.nanoTime();
//        divPageT += t2 - t1;
        if (pages[key].full()) {
            submitPage(cIdx, pIdx, pages[key]);
        }
//        submitT += System.nanoTime() - t2;
    }

    /**
     * 提交到写线程
     */
    private void submitPage(int cIdx, int pIdx, MyPage page) {
        MyValuePage valuePage = (MyValuePage) page;
        bq.put(new WriteTask(
                valuePage.copy(),
                Constant.getPath(tId, table.index, cIdx, pIdx)
        ));
//        write(Constant.getPath(tId, table.index, cIdx, pIdx), page);
        page.clean();
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
                if (pages[key].empty()) continue;
                submitPage(cIdx, pIdx, pages[key]);
            }
        }
    }

    private int getKey(int colIndex, int pageIndex) {
        return colIndex * Constant.PAGE_COUNT + pageIndex;
    }
}
