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
        System.out.printf("Running: %d, %d\n", block.tableIndex, block.blockIndex);
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

    private MyValuePage[][] genPages(int colCount) {
        System.out.printf("Column Count: %d\n", colCount);
        MyValuePage[][] pages = new MyValuePage[colCount][Constant.PAGE_COUNT];
        for (int i = 0; i < colCount; ++i) {
            for (int j = 0; j < Constant.PAGE_COUNT; ++j) {
                pages[i][j] = new MyValuePage();
                pages[i][j].tableIndex = block.tableIndex;
                pages[i][j].blockIndex = block.blockIndex;
                pages[i][j].columnIndex = i;
                pages[i][j].pageIndex = j;
            }
        }
        return pages;
    }

    public void trans(MyBlock block, MappedByteBuffer buffer) {
        MyValuePage[][] pages = null;
        byte b;
        if (block.blockIndex == 0) {
            int colCount = 0;
            StringBuilder colName = new StringBuilder();
            while ((b = buffer.get()) > 0) {
                // 去除文件头的列名
                if (b == 44 || b == 10) {
                    System.out.println("colname: " + colName.toString());
                    table.colIndexMap.put(colName.toString(), colCount);
                    colName = new StringBuilder();
                    if (b == 10) {
                        break;
                    }
                    colCount++;
                } else {
                    colName.append((char) b);
                }
            }
            pages = genPages(colCount + 1);
            // Todo: 根据取的列做初始化
        } else {
            while (buffer.hasRemaining()) {
                b = buffer.get();
                if (b == 10) {
                    break;
                }
                block.addBeginByte(b);
            }
        }
        while (buffer.hasRemaining()) {
            handleByte(pages, buffer.get());
        }
        finish(buffer);
    }

    private long input;
    private double inputD;
    private int nowColIndex;
    private boolean isDouble;
    private int maxDataLen;

    private void handleByte(MyValuePage[][] pages, byte b) {
        if (b >= 48) {
            input = input * 10 + b - 48;
            maxDataLen++;
        } else if (b == 46) {
            // 小数点
            isDouble = true;
            inputD = input;
            maxDataLen = 0;
//            System.out.println("Has Double!!");
        } else {
            if (isDouble) {
                inputD += input * Math.pow(0.1, maxDataLen);
            }
            int pageIndex = Constant.getPageIndex(input);
            putData(pages[nowColIndex][pageIndex]);
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
            writePool.execute(
                    table,
                    Constant.getPath(page),
                    page.byteBuffer
            );
            page.byteBuffer = null;
        }
    }

    private void finish(MappedByteBuffer bb) {
        // 最后一块读完
        if (table.readCount.get() == table.blockCount - 1) {
            // Todo: 处理块合并剩下的

        }
        table.addReadCount();
        Cleaner cl = ((DirectBuffer) bb).cleaner();
        if (cl != null) {
            cl.clean();
        }
    }
}
