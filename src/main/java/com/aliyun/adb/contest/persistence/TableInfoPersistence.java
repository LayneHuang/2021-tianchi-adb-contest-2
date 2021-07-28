package com.aliyun.adb.contest.persistence;

import com.aliyun.adb.contest.Constant;
import com.aliyun.adb.contest.page.MyTable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

/**
 * Table & Page持久化
 */
public class TableInfoPersistence {


    public boolean readLoaded() {
        Path path = Constant.getGlobalPath();
        return path.toFile().exists();
    }

    public void saveTableInfo(List<MyTable> tables) {
        ByteBuffer buffer = ByteBuffer.allocate(Constant.WRITE_SIZE);
        try (FileChannel fileChannel = FileChannel.open(
                Constant.getGlobalPath(),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        )) {
            // 表个数
            buffer.putInt(tables.size());
            for (MyTable table : tables) {
                // 表名
                buffer.putInt(table.name.length());
                for (int i = 0; i < table.name.length(); ++i) buffer.putChar(table.name.charAt(i));
                // 表行数量
                buffer.putInt(table.dataCount);
                // 表列个数
                int colSize = table.colIndexMap.size();
                buffer.putInt(colSize);
                table.colIndexMap.forEach((key, value) -> {
                    // 列序号
                    buffer.putInt(value);
                    // 列名
                    buffer.putInt(key.length());
                    for (int i = 0; i < key.length(); ++i) buffer.putChar(key.charAt(i));
                });
                buffer.flip();
                fileChannel.write(buffer);
                buffer.clear();

                // page 个数
                for (int threadIdx = 0; threadIdx < Constant.THREAD_COUNT; ++threadIdx) {
                    for (int pIdx = 0; pIdx < Constant.PAGE_COUNT; ++pIdx) {
                        for (int cIdx = 0; cIdx < colSize; ++cIdx) {
                            buffer.putInt(table.pageCounts[threadIdx][pIdx][cIdx]);
                            if (!buffer.hasRemaining()) {
                                buffer.flip();
                                fileChannel.write(buffer);
                                buffer.clear();
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loadTableInfo(List<MyTable> tables, Map<String, Integer> indexMap) {
        System.out.println("---------------------- second load -------------------------");
        ByteBuffer buffer = ByteBuffer.allocate((int) Constant.MAPPED_SIZE);
        try (FileChannel fileChannel = FileChannel.open(
                Constant.getGlobalPath(),
                StandardOpenOption.READ)) {
            // 读第一次
            fileChannel.read(buffer);
            buffer.flip();

            // 表个数
            int tableSize = buffer.getInt();
            System.out.println("tableSize:" + tableSize);
            for (int i = 0; i < tableSize; ++i) {
                MyTable table = new MyTable();
                tables.add(table);
                table.index = i;
                // 表名
                int nameLen = buffer.getInt();
                StringBuilder tempName = new StringBuilder();
                for (int j = 0; j < nameLen; ++j) tempName.append(buffer.getChar());
                table.name = tempName.toString();
                System.out.println("name:" + table.name);
                indexMap.put(table.name, table.index);
                // 表行数量
                table.dataCount = buffer.getInt();
                System.out.println("dataCount:" + table.dataCount);
                int colSize = buffer.getInt();
                // 列名
                System.out.println("col size:" + colSize);
                for (int j = 0; j < colSize; ++j) {
                    int colIdx = buffer.getInt();
                    int colNameLen = buffer.getInt();
                    tempName = new StringBuilder();
                    for (int k = 0; k < colNameLen; ++k) tempName.append(buffer.getChar());
                    String colName = tempName.toString();
                    table.colIndexMap.put(colName, colIdx);
                    System.out.println("colIdx:" + colIdx + ",colName:" + colName);
                }
                // page 个数
                for (int threadIdx = 0; threadIdx < Constant.THREAD_COUNT; ++threadIdx) {
                    for (int pIdx = 0; pIdx < Constant.PAGE_COUNT; ++pIdx) {
                        for (int cIdx = 0; cIdx < colSize; ++cIdx) {
                            if (!buffer.hasRemaining()) {
                                buffer.flip();
                                fileChannel.read(buffer);
                            }
                            table.pageCounts[threadIdx][pIdx][cIdx] = buffer.getInt();
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("---------------------- load end --------------------------");
    }

}
