package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.Constant;
import com.aliyun.adb.contest.page.MyBlock;
import com.aliyun.adb.contest.page.MyTable;
import com.aliyun.adb.contest.page.MyValuePage;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class ReadTask implements Runnable {
    private MyTable table;

    private MyBlock block;

    private Path path;

    private WritePool writePool;

    ReadTask(Path path, MyTable table, MyBlock block, WritePool writePool) {
        this.path = path;
        this.table = table;
        this.block = block;
        this.writePool = writePool;
    }

    @Override
    public void run() {
        try (FileChannel channel = FileChannel.open(path)) {
            MappedByteBuffer buffer = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    block.begin,
                    block.getSize()
            );
            trans(block, buffer.load());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void trans(MyBlock block, MappedByteBuffer buffer) {
        Map<String, MyValuePage> pages = new HashMap<>();
        byte b;
        if (block.blockIndex == 0) {
            StringBuilder colName = new StringBuilder();
            while (buffer.hasRemaining()) {
                b = buffer.get();
                // 去除文件头的列名
                if (b == 10) {
                    String[] names = colName.toString().split(",");
                    for (int i = 0; i < names.length; ++i) {
                        table.colIndexMap.put(names[i], i);
                    }
                    table.pageCounts = new int[table.blockCount][Constant.PAGE_COUNT][names.length];
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
            handleByte(pages, buffer.get());
        }
        finish(pages, buffer);
    }

    private long input;
    private double inputD;
    private int nowColIndex;
    private boolean isDouble;
    private int maxDataLen;

    private void handleByte(Map<String, MyValuePage> pages, byte b) {
        if (b >= 48) {
            input = input * 10 + b - 48;
            maxDataLen++;
        } else if (b == 46) {
            // 小数点
            isDouble = true;
            System.out.println("HAS DOUBLE!!!");
            inputD = input;
            maxDataLen = 0;
        } else {
            if (isDouble) {
                inputD += input * Math.pow(0.1, maxDataLen);
            }
            if (maxDataLen > 0) {
                putData(getPage(pages, nowColIndex, input));
            }
            maxDataLen = 0;
            isDouble = false;
            nowColIndex = (b == 44) ? (nowColIndex + 1) : 0;
            input = 0;
        }
    }

    private void putData(MyValuePage page) {
        if (isDouble) {
            putDouble();
        } else {
            putLong(page);
        }
    }

    private void putDouble() {
    }

    private void putLong(MyValuePage page) {
        page.add(input);
        if (!page.byteBuffer.hasRemaining()) {
            table.allPageCount.incrementAndGet();
            writePool.execute(
                    table,
                    Constant.getPath(page),
                    page.byteBuffer
            );
            page.byteBuffer = null;
        }
    }

    private void finish(Map<String, MyValuePage> pages, MappedByteBuffer bb) {
        setCurToBlock();
        // 最后一块读完
        int readCount = table.readCount.incrementAndGet();
//        System.out.println("read: " + table.index + " " + table.readCount.get() + " " + table.blockCount);
        if (readCount >= table.blockCount) {
            for (int i = 0; i < table.blockCount - 1; ++i) {
                MyBlock block = table.blocks[i];
                MyBlock nxtBlock = table.blocks[i + 1];
//                System.out.println("mer: " + i + " " + nxtBlock.beginLen);
                if (nxtBlock.beginLen <= 0 && block.lastInput == 0) continue;
                long firstValue = block.lastInput * (long) Math.pow(10, nxtBlock.firstNumLen) + nxtBlock.begins[0];
                putData(getPage(pages, block.lastColIndex, firstValue));
                for (int j = 1; j < nxtBlock.beginLen; ++j) {
//                    System.out.println("mer: " + nxtBlock.begins[j]);
                    putData(getPage(pages, block.lastColIndex + j, nxtBlock.begins[j]));
                }
            }
            table.blocks = null;
            System.out.println("table: " + table.index + " read finished " + (table.readCount.get() + 1));
        }
        pages.forEach((key, page) -> {
            if (page.byteBuffer == null) return;
            table.allPageCount.incrementAndGet();
            table.pageCounts[page.blockIndex][page.pageIndex][page.columnIndex] += page.dataCount;
            writePool.execute(
                    table,
                    Constant.getPath(page),
                    page.byteBuffer
            );
        });
        if (readCount >= table.blockCount) {
            table.readFinish = true;
        }
//        System.out.println("read count: " + readCount + " now:" + System.currentTimeMillis());
        Cleaner cl = ((DirectBuffer) bb).cleaner();
        if (cl != null) {
            cl.clean();
        }
    }

    private void setCurToBlock() {
        block.lastColIndex = nowColIndex;
        block.lastInput = input;
        block.lastInputD = inputD;
        block.isD = isDouble;
    }

    private MyValuePage getPage(Map<String, MyValuePage> pages, int colIndex, long value) {
        int pageIndex = Constant.getPageIndex(value);
        String key = getKey(colIndex, pageIndex);
        if (!pages.containsKey(key)) {
            MyValuePage page = new MyValuePage();
            page.tableIndex = block.tableIndex;
            page.blockIndex = block.blockIndex;
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
