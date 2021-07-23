package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.Constant;
import com.aliyun.adb.contest.page.MyBlock;
import com.aliyun.adb.contest.page.MyTable;
import com.aliyun.adb.contest.page.MyValuePage;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class ReadTask implements Runnable {
    private final MyTable table;

    private final MyBlock block;

    private final Path path;

    private final WritePool writePool;

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
            long begin = System.currentTimeMillis();
            // trans(buffer.load());
            // notTrans(buffer.load());
//             notTransNotWrite(buffer);
            transNumberNotWrite(buffer);
            Cleaner cleaner = ((sun.nio.ch.DirectBuffer) buffer).cleaner();
            if (cleaner != null) {
                cleaner.clean();
            }
            System.out.println(block.tableIndex + " " + block.blockIndex +" single block read cost: " + (System.currentTimeMillis() - begin));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void notTransNotWrite(MappedByteBuffer originBuffer) {
//        ByteBuffer buffer = null;
        byte b = 0;
        while (originBuffer.hasRemaining()) {
//            if (buffer == null) {
//                buffer = ByteBuffer.allocate(Constant.WRITE_SIZE);
//            }
            b = originBuffer.get();
//            buffer.put(originBuffer.get());
//            if (!buffer.hasRemaining()) {
//                buffer.clear();
//            }
        }
        System.out.println(b);
        writePool.checkJustCountDown(table);
    }

    private void handleLong(long l){
        // DO NOTHING
    }

    private void transNumberNotWrite(MappedByteBuffer originBuffer) {
//        ByteBuffer buffer = null;
        while (originBuffer.hasRemaining()) {
//            if (buffer == null) {
//                buffer = ByteBuffer.allocate(Constant.WRITE_SIZE);
//            }
            byte b = originBuffer.get();
            if (b >= 48 && b < 58) {
                input = input * 10 + b - 48;
            } else {
                input = 0;
                handleLong(input);
//                buffer.putLong(input);
//                if (!buffer.hasRemaining()) {
//                    buffer.clear();
//                }
            }
        }
        writePool.checkJustCountDown(table);
    }

    private void notTrans(MappedByteBuffer originBuffer) {
        ByteBuffer buffer = null;
        while (originBuffer.hasRemaining()) {
            if (buffer == null) {
                buffer = ByteBuffer.allocate(Constant.WRITE_SIZE);
            }
            buffer.put(originBuffer.get());
            if (!buffer.hasRemaining()) {
                writePool.execute(table, Constant.getPath(table.index, 0, block.blockIndex, 0), buffer);
                buffer = null;
            }
        }
    }

    private void trans(MappedByteBuffer buffer) {
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
                putData(getPage(pages, nowColIndex));
            }
            maxDataLen = 0;
            isDouble = false;
            nowColIndex = (b == 44) ? (nowColIndex + 1) : 0;
            input = 0;
        }
    }

    private void putData(MyValuePage page) {
        if (input == 0) return;
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

    private void finish(Map<String, MyValuePage> pages, MappedByteBuffer bb) {
        setCurToBlock();
        // 最后一块读完
        int readCount = table.readCount.incrementAndGet();
        if (readCount >= table.blockCount) {
            mergeBlocks(pages);
            table.blocks = null;
            System.out.println("table: " + table.index + " read finished " + (table.readCount.get() + 1));
        }
        flushRestPage(pages);
        if (readCount >= table.blockCount) {
            table.readFinish = true;
            System.out.println("read finish, now: " + System.currentTimeMillis());
        }
        Cleaner cl = ((DirectBuffer) bb).cleaner();
        if (cl != null) {
            cl.clean();
        }
    }

    /**
     * 记录每个 Block 最后一个值, 用来合并 Block
     */
    private void setCurToBlock() {
        block.lastColIndex = nowColIndex;
        block.lastInput = input;
        block.lastInputD = inputD;
        block.isD = isDouble;
    }

    /**
     * 处理 Blocks 的边缘值
     */
    private void mergeBlocks(Map<String, MyValuePage> pages) {
        for (int i = 0; i < table.blockCount - 1; ++i) {
            MyBlock block = table.blocks[i];
            MyBlock nxtBlock = table.blocks[i + 1];
            input = block.lastInput * (long) Math.pow(10, nxtBlock.firstNumLen) + nxtBlock.begins[0];
            putData(getPage(pages, block.lastColIndex));
            for (int j = 1; j < nxtBlock.beginLen; ++j) {
                input = nxtBlock.begins[j];
                putData(getPage(pages, block.lastColIndex + j));
            }
        }
        table.blocks = null;
    }

    /**
     * 剩下的 Page 落盘
     */
    private void flushRestPage(Map<String, MyValuePage> pages) {
        pages.forEach((key, page) -> {
            if (page.byteBuffer == null) return;
            table.pageCounts[page.blockIndex][page.pageIndex][page.columnIndex] += page.dataCount;
            writePool.execute(
                    table,
                    Constant.getPath(page),
                    page.byteBuffer
            );
            page.byteBuffer = null;
        });
    }

    private MyValuePage getPage(Map<String, MyValuePage> pages, int colIndex) {
        int pageIndex = Constant.getPageIndex(input);
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
