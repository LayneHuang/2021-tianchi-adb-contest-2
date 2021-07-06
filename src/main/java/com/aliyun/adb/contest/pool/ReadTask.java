package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.Constant;
import com.aliyun.adb.contest.page.MyBlock;
import com.aliyun.adb.contest.page.MyValuePage;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;

public class ReadTask implements Runnable {

    private MyBlock block;

    private Path path;

    private MyValuePage[] pages;

    private BlockingQueue<MyValuePage> bq;

    ReadTask(Path path, MyBlock block, BlockingQueue<MyValuePage> bq) {
        this.block = block;
        this.path = path;
        this.pages = new MyValuePage[Constant.PAGE_COUNT];
        this.bq = bq;
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
        byte b;
        if (block.blockIndex == 0) {
            while ((b = buffer.get()) != 10) {
                // 去除文件头的列名
                if (b == 44) {
                }
            }
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
            handleByte(block, buffer.get());
        }
        finish(buffer);
    }

    private long input;
    private double inputD;
    private int inputIndex;
    private boolean isDouble;
    private int maxDataLen;

    private void handleByte(MyBlock block, byte b) {
        if (b >= 48) {
            input = input * 10 + b - 48;
            maxDataLen++;
        } else if (b == 46) {
            // 小数点
            isDouble = true;
            inputD = input;
            maxDataLen = 0;
            System.out.println("Has Double!!");
        } else {
            if (isDouble) {
                inputD += input * Math.pow(0.1, maxDataLen);
            }
            putData();
            maxDataLen = 0;
            isDouble = false;
            inputIndex = (b == 44) ? (inputIndex + 1) : 0;
            input = 0;
        }
    }

    private void putData() {
        if (isDouble) {
            putDouble();
        } else {
            putLong();
        }
    }

    private void putDouble() {
    }

    private void putLong() {
        int pageIndex = Constant.getPageIndex(input);
        pages[pageIndex].add(input);
        if (!pages[pageIndex].byteBuffer.hasRemaining()) {
            try {
                bq.put(pages[pageIndex]);
            } catch (InterruptedException e) {
                System.out.println("GG, PUT BQ FAILURE");
                e.printStackTrace();
            }
        }
    }

    private static void finish(MappedByteBuffer bb) {
        Cleaner cl = ((DirectBuffer) bb).cleaner();
        if (cl != null) {
            cl.clean();
        }
    }
}
