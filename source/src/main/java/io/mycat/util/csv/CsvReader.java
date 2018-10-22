package io.mycat.util.csv;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * ${desc}
 * @author zhuqiang
 * @version 1.0.1 2018/10/20 17:08
 * @date 2018/10/20 17:08
 * @since 1.0
 */
public abstract class CsvReader {
    /**
     * 主要功能：无特殊字符，逗号 分隔符，解析 csv 文件
     * 难点：边界判定。
     * 1. 逗号边界判定截取列长度容易把逗号截住
     * 2. 最后一列如果没有换行符的话，容易丢失最后一列的内容
     * 3. 当列 buffer 需要扩容的时候，容易造成混乱
     * ----------------------------------------------------
     * 以上功能完成之后，开始支持 MappedByteBuffer 的改造
     * 思路：也就是找到 传统 inputStream 和 MappedByteBuffer 读取数据源的不同之处
     * 1. 分离从数据源中读取数据的函数
     * 2. 将读取处理下沉到子类，分为普通 io 和 MappedByteBuffer 实现。MappedByteBuffer 实现是高效的实现
     */

    /**
     * 数据缓冲区：为了兼容高效的 MappedByteBuffer
     * 该 buffer 不允许操作 buffer.array() 因为不支持
     */
    protected ByteBuffer buffer;
    private byte currentLetter;
    protected boolean inited;
    // 列开始标识，也意味着正在一列的解析中
    private boolean startedColumn;
    // 列开始在 buffer 中的位置
    private int startedColumnPosition;

    // 列开始标识，也意味着正在一列的解析中
    private boolean startedRecord;
    // 是否有更多的数据可处理，当输入源无数据的时候标识已经处理完
    protected boolean hasMoreData = true;

    // 该 buffer 可以使用 array ，因为不涉及到兼容问题
    protected ByteBuffer columnBuffer;
    // 存放一行的记录
    List<byte[]> values = new ArrayList<>();

    /**
     * 需要完成的工作有：
     * - 数据源的赋值
     * - 各种配置的赋值和初始化
     * @throws FileNotFoundException
     */
    protected abstract void init() throws IOException;

    public List<byte[]> next() {
        return values;
    }

    public boolean hashNext() throws IOException {
        // 一个 byte 的读取，然后判定边界
        startedRecord = true;
        values.clear();

        while (hasMoreData && startedRecord) {
            // 在读取之前都必须检查是否还有数据
            // 如果没有数据了，需要读取
            if (!buffer.hasRemaining()) {
                checkDataLength();
                continue;
            }
            currentLetter = buffer.get();

            // 当遇到换行符号的时候，有以下几个场景
            // - 在 windows 下的换行符为 /r/n,经过 noTextQualifierParse 解析后，只能处理 第一个 /r
            //      需要额外的跳过这一个换行符，因为也是需要去掉的
            if (currentLetter == Letters.LF || currentLetter == Letters.CR) {
                recordDelimiterParse();
            } else {
                // 先做无特殊字符的功能
                noTextQualifierParse();
            }
        }

        // 需要做扫尾处理，有以下集中情况
        // - 当没有数据可处理的时候，hasMoreData 会修改为 false,所有循环都会停止
        //     那么残留在 columnBuffer 中的数据需要扫尾
        if (startedColumn) {
            endColumn();
            endRecord();
        }

        return values.size() != 0;
    }

    private void recordDelimiterParse() {
        // 暂时空着就好，不处理就是跳过
    }


    private void noTextQualifierParse() throws IOException {
        // 一次方法的调用意味着一个列的开始
        startedColumn = true;
        // 记录列开始的 position，以便从 buffer 中读取出这一列的值
        // 因为在前面已经读取过一个字符了，这里需要回退一个
        buffer.position(buffer.position() - 1);
        startedColumnPosition = buffer.position();
        columnBuffer.clear();

        while (hasMoreData && startedColumn) {
            if (!buffer.hasRemaining()) {
                checkDataLength();
                continue;
            }
            currentLetter = buffer.get();

            // 读取到了列分隔符，标识一列的结束
            if (currentLetter == Letters.COMMA) {
                endColumn();
            }
            // 读取到了换行符，标识一行数据的结束，但是要注意在 windows 中是 /r/n 后面一个需要额外的处理
            // 这里应该不能采取读取两个字符来对比，因为会加大解析逻辑的复杂度
            else if (currentLetter == Letters.CR || currentLetter == Letters.LF) {
                endColumn();
                endRecord();
            }
        }
    }

    /**
     * 该函数只能在找到分隔符的时候才能调用，只能是确定是一列的结尾的时候才能被调用
     */
    private void endColumn() {
        // 获取这一列的数据

        // 必须保证是在列解析中，什么情况下不是呢？

        // 需要分情况
        // 当一列的数据在 buffer 都存在的时候，可以直接把一列数据都获取出来，不需要中转
        // 所以当 列中转 存储为 0 的时候，就符合这一设定
        if (columnBuffer.position() == 0) {
            int position = buffer.position();
            // 跳过分隔符
            int lastLetter = position - 1;
            byte[] columnValue = new byte[lastLetter - startedColumnPosition];
            buffer.position(startedColumnPosition);
            buffer.get(columnValue);
            buffer.position(position);
            values.add(columnValue);
        } else {
            // 当列中有数据的时候，情况如下
            // - 解析列数据直到当前 buffer 耗尽都没有遇到换行符或则分隔符

            // 需要把当前的已经读取到的数据存入列 buffer 中，然后把列 buffer 中的数据全部读取出来
            // 在这之前需要考虑一个问题，当前列 buffer 是否能装得下要写入的数据
            updateCurrentColumnBuffer();
            // 跳过分隔符
            int lastLetter = columnBuffer.position() - 1;
            // 从列 buffer 中读取数据
            byte[] columnValue = new byte[lastLetter];
            System.arraycopy(columnBuffer.array(), 0, columnValue, 0, columnValue.length);
            values.add(columnValue);
            // 清空列中的数据以便下一次使用
            columnBuffer.clear();
        }

        startedColumn = false;
    }

    private void endRecord() {
        startedRecord = false;
    }

    private void updateCurrentColumnBuffer() {
        int position = buffer.position();
        int length = position - startedColumnPosition;
        // 当前列 buffer 剩余容量不够装下要更新的值的时候，需要扩容
        if (columnBuffer.remaining() < length) {
            // 需要扩容，确定新的扩容容量
            int oldCapacity = columnBuffer.capacity();
            int newCapacity = oldCapacity + length;
            ByteBuffer newColumnBuffer = ByteBuffer.allocate(newCapacity);
            columnBuffer.flip();
            newColumnBuffer.put(columnBuffer.array(), 0, columnBuffer.limit());
            columnBuffer = newColumnBuffer;
//            System.out.println("列扩容");
        }
        if (length != 0) {
            byte[] bytes = new byte[length];
            buffer.position(startedColumnPosition);
            buffer.get(bytes);
            columnBuffer.put(bytes);
            // 数据 copy 之后需要更新列开始在 buffer 中的位置
            // 这里不能更新，因为在 hashNext 中扫尾的时候还会用到 startedColumnPosition 的位置，
            // 当该函数在 checkDataLength 调用之后，判定为无更多数据，则会进入扫尾模式
            // 扫尾的时候会进入到 endColumn 中，再次获取 startedColumnPosition 的位置
            // 这里更新为了最新的，那么 buffer.position() - startedColumnPosition 的时候就获取不到最后一个字符
            // startedColumnPosition = buffer.position();
        }
    }

    private void checkDataLength() throws IOException {
        // 在没有数据的时候，可能有以下几种情况
        // - 在解析中 buffer 耗尽的时候
        // 所以需要把之前已经解析的数据备份存档
        updateCurrentColumnBuffer();
        hasMoreData = this.doCheckDataLength();
        if (hasMoreData) {
            // 重新读取之后，列开始只能是从 0 开始
            startedColumnPosition = 0;
        }
    }

    protected abstract boolean doCheckDataLength() throws IOException;

    private class Letters {
        public static final byte LF = '\n';

        public static final byte CR = '\r';

        public static final byte COMMA = ',';
    }
}
