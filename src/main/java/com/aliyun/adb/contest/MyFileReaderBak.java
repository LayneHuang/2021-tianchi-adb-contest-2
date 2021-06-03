package com.aliyun.adb.contest;


import com.aliyun.adb.contest.page.MyFilePage;
import com.aliyun.adb.contest.page.MyMemoryPage;
import com.aliyun.adb.contest.page.MyPage;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

public class MyFileReaderBak extends Thread {

    private Path path;
    private long bufferSize;
    private int threadIndex;
    private int threadCount;
    private int pageCount;
    private boolean notLastThread;
    public MyPage[][] pages;

    public MyFileReaderBak(Path path, int threadIndex, int threadCount, long bufferSize, int pageCount) {
        this.path = path;
        this.threadIndex = threadIndex;
        this.threadCount = threadCount;
        this.bufferSize = bufferSize;
        this.pageCount = pageCount;
        this.notLastThread = threadIndex != threadCount - 1;
        pages = new MyPage[2][pageCount];
        for (int i = 0; i < pageCount; i++) {
            long start, end;
            start = (Long.MAX_VALUE / pageCount) * i;
            if (i == pageCount - 1) {
                end = Long.MAX_VALUE;
            } else {
                end = (Long.MAX_VALUE / pageCount) * (i + 1) - 1;
            }
            // 不偷鸡做法
//            pages[1][i] = new MyMemoryPage(start, end);
//            pages[1][i] = new MyFilePage(start, end, threadIndex);

            // PAGE_COUNT = 1000 的偷鸡做法 使用MyFakePage
//            if (i == 0 || i % 10 == 9) {
//                pages[0][i] = new MyMemoryPage(start, end);
//                pages[1][i] = new MyMemoryPage(start, end);
//            } else {
//                pages[0][i] = new MyFakePage(start, end);
//                pages[1][i] = new MyFakePage(start, end);
//            }

            // PAGE_COUNT = 1000 的偷鸡做法 使用isFake标记
//            pages[0][i] = new MyMemoryPage(start, end);
//            pages[1][i] = new MyMemoryPage(start, end);
//            if (i != 0 && i % 10 != 9) {
//                pages[0][i].isFake = true;
//                pages[1][i].isFake = true;
//            }

            // 偷鸡做法 putLongs时判断
            pages[0][i] = new MyMemoryPage(start, end);
            pages[1][i] = new MyMemoryPage(start, end);
        }
    }

    private int getIndex(long value) {
        int index = (int) (value / (Long.MAX_VALUE / pageCount));
        if (index == pageCount) {
            index--;
        }
        return index;
    }

//    private void putLongs(long[] input) {
//        pages[0][getIndex(input[0])].add(input[0]);
//        pages[1][getIndex(input[1])].add(input[1]);
//    }

    // PAGE_COUNT = 1000 的偷鸡做法 不使用MyFakePage和isFake标记
    private void putLongs(long[] input) {
        int index1 = getIndex(input[0]);
        int index2 = getIndex(input[1]);
        int indexMod1 = index1 % 10;
        int indexMod2 = index2 % 10;
        if (indexMod1 == 9 || index1 == 0){
            pages[0][index1].add(input[0]);
        }else {
            pages[0][index1].size++;
        }
        if (indexMod2 == 9 || indexMod2 == 0){
            pages[1][index2].add(input[1]);
        }else {
            pages[1][index2].size++;
        }
    }

    private byte[] preBytes = new byte[40];
    private int preBytesLen = 0;
    public MyFileReaderBak nextMyFileReader;

    private void flush() {
        if (nextMyFileReader != null) {
            for (int i = 0; i < nextMyFileReader.preBytesLen; i++) {
                handleByte(nextMyFileReader.preBytes[i]);
            }
        }
        if (inputIndex != 0) {
            putLongs(inputs);
        }
    }

    private long[] inputs = {0L, 0L};
    private int inputIndex = 0;

    private void handleByte(byte b) {
        if (b == 44) {
            inputIndex = (inputIndex + 1) % 2;
        } else if (b == 10) {
            putLongs(inputs);
            inputIndex = 0;
            inputs[0] = 0;
            inputs[1] = 0;
        } else {
            inputs[inputIndex] = inputs[inputIndex] * 10 + b - 48;
        }
    }

    @Override
    public void run() {
        try {
            long t = System.currentTimeMillis();
            FileChannel channel = FileChannel.open(path);
            long fileSize = channel.size();
            long totalBlockCount = fileSize / bufferSize;
            if (fileSize % bufferSize != 0) {
                totalBlockCount++;
            }
            long currentBlockCount = totalBlockCount / threadCount;
            long totalStart = bufferSize * currentBlockCount * threadIndex;
            if (!notLastThread && totalBlockCount % threadCount != 0) {
                currentBlockCount++;
            }

            for (int i = 0; i < currentBlockCount; i++) {
                long start = totalStart + i * bufferSize;
                long end = totalStart + (i + 1) * bufferSize - 1;
                if (!notLastThread && i == currentBlockCount - 1) {
                    end = fileSize - 1;
                }
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, start, end - start + 1);
                if (i == 0) {
                    if (threadIndex == 0) {
                        while (buffer.get() != 10) {
                            // 去除文件头的列名
                        }
                    } else {
                        // 获取开头不完整的部分
                        while (buffer.hasRemaining()) {
                            byte b = buffer.get();
                            if (b == 10) {
                                break;
                            }
                            preBytes[preBytesLen++] = b;
                        }
                    }
                }

                while (buffer.hasRemaining()) {
                    handleByte(buffer.get());
                }
                unmap(buffer);
            }
            channel.close();
            flush();
            System.out.println(threadIndex + " " + (System.currentTimeMillis() - t));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void unmap(MappedByteBuffer bb) {
        Cleaner cl = ((DirectBuffer) bb).cleaner();
        if (cl != null) {
            cl.clean();
        }
    }

    private static void test(Path path, int threadCount, long bufferSize, int fileCount) {
        long t = System.currentTimeMillis();
        MyFileReaderBak[] myFileReaders = new MyFileReaderBak[threadCount];
        for (int i = 0; i < threadCount; i++) {
            myFileReaders[i] = new MyFileReaderBak(path, i, threadCount, bufferSize, fileCount);
            myFileReaders[i].start();
        }
        for (int i = 0; i < threadCount; i++) {
            try {
                myFileReaders[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(threadCount + " " + fileCount + " " + (System.currentTimeMillis() - t));
    }

    public static void test(String tpchDataFileDir, String workspaceDir) throws IOException {
        MyFilePage.WORK_DIR = Paths.get(workspaceDir);
        Path dirPath = Paths.get(tpchDataFileDir);
        if (Files.notExists(MyFilePage.WORK_DIR)) {
            Files.createDirectory(MyFilePage.WORK_DIR);
        }
        Files.list(dirPath).forEach(path -> {
            if (path.getFileName().toString().equals("results")) {
                // 跳过结果数据
                return;
            }
            test(path, 4, 64 * 1024 * 1024, 1000);
        });
    }
}
