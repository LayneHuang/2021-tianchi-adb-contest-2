package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.Constant;
import com.aliyun.adb.contest.cache.MyBlockingQueueCache;
import com.aliyun.adb.contest.page.MyBlock;
import com.aliyun.adb.contest.page.MyTable;
import com.aliyun.adb.contest.page.MyValuePage;
import sun.misc.Cleaner;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

public class ReadThread extends Thread {

    public MyTable[] allTables;

    public MyTable table;

    public MyBlockingQueueCache bq;

    private final Map<String, MyValuePage> pages = new HashMap<>();

    private int blockCountInThread;

    /**
     * 线程序号
     */
    public int tId;

    @Override
    public void run() {
        for (MyTable table : allTables) {
            this.table = table;
            try (FileChannel channel = FileChannel.open(table.path)) {

                long fileSize = channel.size();
//            System.out.println("fileSize: " + fileSize);
                // 分成多少块
                int DEFAULT_BLOCK_COUNT = (int) (fileSize / Constant.MAPPED_SIZE)
                        + (fileSize % Constant.MAPPED_SIZE == 0 ? 0 : 1);

                // 每个线程读多少块
                int BLOCK_PER_THREAD = (DEFAULT_BLOCK_COUNT / Constant.THREAD_COUNT)
                        + (DEFAULT_BLOCK_COUNT % Constant.THREAD_COUNT == 0 ? 0 : 1);

                int beginBIdx = tId * BLOCK_PER_THREAD;

                int endBIdx = Math.min(beginBIdx + BLOCK_PER_THREAD, DEFAULT_BLOCK_COUNT);

                if (beginBIdx >= DEFAULT_BLOCK_COUNT) {
                    bq.put(new WriteTask());
                    return;
                }

                table.initBlocks(DEFAULT_BLOCK_COUNT);
                System.out.println("DEFAULT_BLOCK_COUNT: " + DEFAULT_BLOCK_COUNT + ", BLOCK_PER_THREAD: " + BLOCK_PER_THREAD + ", beginBIdx: " + beginBIdx + ", endIndex: " + endBIdx);

                blockCountInThread = endBIdx - beginBIdx;

                for (int i = 0; i < blockCountInThread; ++i) {

                    int bIdx = beginBIdx + i;

                    long begin = Constant.MAPPED_SIZE * bIdx;

                    long end = Math.min(begin + Constant.MAPPED_SIZE, fileSize);

                    // System.out.println("block " + bIdx + ", begin: " + begin + " ,end: " + end);

                    MappedByteBuffer buffer = channel.map(
                            FileChannel.MapMode.READ_ONLY,
                            begin,
                            end - begin
                    );
                    trans(i, table.blocks[bIdx], buffer);
//                notTrans(buffer);
                    // notTransNotWrite(buffer);
//                transNumberNotWrite(buffer);
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
    }

    public void notTrans(MappedByteBuffer buffer) {
        long sum = 0;
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            sum += b;
        }
        System.out.println(sum);
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
                    table.pageCounts = new int[Constant.THREAD_COUNT][Constant.PAGE_COUNT][names.length];
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
            handleByte(buffer.get());
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
                putData(getPage(nowColIndex));
            }
            maxDataLen = 0;
            nowColIndex = (b == 44) ? (nowColIndex + 1) : 0;
            input = 0;
        }
    }

    private void putData(MyValuePage page) {
        if (input == 0) return;
        page.add(input);
        if (!page.byteBuffer.hasRemaining()) {
            bq.put(new WriteTask(page.byteBuffer, Constant.getPath(page)));
            page.byteBuffer = null;
        }
    }

    private void finish() {
        // 最后一块读完
        int readCount = table.readCount.addAndGet(blockCountInThread);
        if (readCount >= table.blockCount) {
            mergeBlocks();
            table.blocks = null;
            System.out.println("table: " + table.index + " read finished, " + readCount + ", " + table.blockCount);
        }
        flushRestPage();
        bq.put(new WriteTask());
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
            putData(getPage(block.lastColIndex));
            for (int j = 1; j < nxtBlock.beginLen; ++j) {
                input = nxtBlock.begins[j];
                putData(getPage(block.lastColIndex + j));
            }
        }
        table.blocks = null;
    }

    /**
     * 剩下的 Page 落盘
     */
    private void flushRestPage() {
        pages.forEach((key, page) -> {
            if (page.byteBuffer == null) return;
            table.pageCounts[page.threadIndex][page.pageIndex][page.columnIndex] += page.dataCount;
            bq.put(new WriteTask(page.byteBuffer, Constant.getPath(page)));
            page.byteBuffer = null;
        });
    }

    private MyValuePage getPage(int colIndex) {
        int pageIndex = Constant.getPageIndex(input);
        String key = getKey(colIndex, pageIndex);
        if (!pages.containsKey(key)) {
            MyValuePage page = new MyValuePage();
            page.tableIndex = table.index;
            page.threadIndex = tId;
            page.columnIndex = colIndex;
            page.pageIndex = pageIndex;
            page.dataCount = 0;
            pages.put(key, page);
            return page;
        }
        return pages.get(key);
    }

    private String getKey(int colIndex, int pageIndex) {
        return colIndex + ":" + pageIndex;
    }
}
