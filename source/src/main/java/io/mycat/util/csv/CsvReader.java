package io.mycat.util.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * csv 格式文件读取解析公共逻辑;
 * <pre>
 * 无特殊字符解析：逗号 分隔符，CR / LF (/r/n) 换行
 * 难点：边界判定。
 *  1. 逗号边界判定截取列长度容易把逗号截住
 *  2. 最后一列如果没有换行符的话，容易丢失最后一列的内容
 *  3. 当列 buffer 需要扩容的时候，容易造成混乱
 * ----------------------------------------------------
 * 以上功能完成之后，开始支持 MappedByteBuffer 的改造
 * 思路：也就是找到 传统 inputStream 和 MappedByteBuffer 读取数据源的不同之处
 *  1. 分离从数据源中读取数据的函数
 *  2. 将读取处理下沉到子类，分为普通 io 和 MappedByteBuffer 实现。MappedByteBuffer 实现是高效的实现
 * ----------------------------------------------------
 * 继续支持：带特殊字符的解析处理，符合 RFC 4180标准的 双引号列（字段）
 *  RFC 4180标准：https://en.wikipedia.org/wiki/Comma-separated_values
 *  - 以（CR / LF）字符结尾的MS-DOS样式行（最后一行可选）。
 *  - 每条记录“应该”包含相同数量的逗号分隔字段
 *  - 带有特殊符号的字段必须被双引号包裹，特殊字符包含换行符，双引号或逗号等一切符号
 *  - 字段中的（双）引号字符必须由两个（双）引号字符表示。
 *    如 [ 我是一个"粉刷匠" ],必须被表现为 [ "我是一个""粉刷匠""" ]
 *    否则不能正常解析
 * </pre>
 * @author zhuqiang
 * @date 2018/10/20 17:08
 */
public abstract class CsvReader {
    private Logger log = LoggerFactory.getLogger(getClass());
    /**

     */

    protected CsvReaderConfig config;
    /**
     * 数据缓冲区：为了兼容高效的 MappedByteBuffer
     * 该 buffer 不允许操作 buffer.array() 因为不支持
     */
    protected ByteBuffer buffer;
    private byte currentLetter;
    protected boolean inited;
    /** 列开始标识，也意味着正在一列的解析中 */
    private boolean startedColumn;
    /** 列开始在 buffer 中的位置 */
    private int startedColumnPosition;

    /** 列开始标识，也意味着正在一列的解析中 */
    private boolean startedRecord;
    /** 是否有更多的数据可处理，当输入源无数据的时候标识已经处理完 */
    protected boolean hasMoreData = true;

    /** 该 buffer 可以使用 array ，因为不涉及到兼容问题 */
    protected ByteBuffer columnBuffer;
    /** 存放一行的记录 */
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
            } else if (currentLetter == Letters.TEXT_QUALIFIER) {
                startedWasQualifier = true;
                textQualifierParse();
            } else {
                startedWasQualifier = false;
                // 先做无特殊字符的功能
                noTextQualifierParse();
            }
        }

        // 需要做扫尾处理，有以下几种情况
        // - 当没有数据可处理的时候，hasMoreData 会修改为 false,所有循环都会停止
        //     那么残留在 columnBuffer 中的数据需要扫尾
        if (startedColumn && columnBuffer.position() != 0) {
            endColumnLast();
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

    private boolean startedWasQualifier = false;

    private void textQualifierParse() throws IOException {
        startedColumn = true;
        columnBuffer.clear();
        startedColumnPosition = buffer.position();

        // 最后一个字母是逃逸的值，也就是可能双引号的第一个引号，也有可能是文本边界
        boolean lastLetterWasEscape = false;

        while (hasMoreData && startedColumn) {
            if (!buffer.hasRemaining()) {
                checkDataLength();
                continue;
            }
            currentLetter = buffer.get();
            // 要处理协议中的特殊字符
            // - "我是一个,粉刷匠","粉刷匠"
            // - "我是一个,粉\r\n刷匠","粉刷匠"
            // - "我是""一个"",粉\r\n刷匠","粉刷匠"

            // 先找到 ", 这种情况，那么必定是分割符了
//            System.out.println((char) currentLetter);
            if (currentLetter == Letters.TEXT_QUALIFIER) {
                if (lastLetterWasEscape) {
                    lastLetterWasEscape = false;
                } else {
                    updateCurrentColumnBuffer();
//                    System.out.println("\r ->   :" + new String(columnBuffer.array(), 0, columnBuffer.position(), "utf8"));
                    // 可能是双引号模式
                    lastLetterWasEscape = true;
                }
            } else {
                // 最后一个是边界符，判定是否是分隔符
                if (lastLetterWasEscape) {
                    if (currentLetter == Letters.COMMA) {
                        endColumn();
                    } else if (currentLetter == Letters.CR || currentLetter == Letters.LF) {
                        endColumn();
                        endRecord();
                    } else {
                        System.out.println("xxx");
                    }
                }
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

    /**
     * 当没有数据可读取的时候，进行的列扫尾处理
     */
    private void endColumnLast() {
        int lastLetter = columnBuffer.position();
        // 当是文本边界符的时候，最后一个一定是 " 号，这个是边界符，需要去掉
        if (startedWasQualifier) {
            lastLetter--;
        }
        byte[] columnValue = new byte[lastLetter];
        System.arraycopy(columnBuffer.array(), 0, columnValue, 0, columnValue.length);
        values.add(columnValue);
        columnBuffer.clear();
        startedColumn = false;
    }

    private void endRecord() {
        startedRecord = false;
    }

    private void updateCurrentColumnBuffer() {
        int position = buffer.position();
        int length = position - startedColumnPosition;
        // 在文本边界模式中：当跳过一个字符的时候，在检查数据长度的时候，startedColumnPosition 就会大于 position
        if (startedColumnPosition < position) {
            // 当前列 buffer 剩余容量不够装下要更新的值的时候，需要扩容
            if (columnBuffer.remaining() < length) {
                // 需要扩容，确定新的扩容容量
                int oldCapacity = columnBuffer.capacity();
                int newCapacity = oldCapacity + length;
                ByteBuffer newColumnBuffer = ByteBuffer.allocate(newCapacity);
                columnBuffer.flip();
                newColumnBuffer.put(columnBuffer.array(), 0, columnBuffer.limit());
                columnBuffer = newColumnBuffer;
                log.debug("列扩容 oldCapacity -> newCapacity : {} -> {}", oldCapacity, newCapacity);
            }
            if (length != 0) {
                byte[] bytes = new byte[length];
                buffer.position(startedColumnPosition);
                buffer.get(bytes);
                columnBuffer.put(bytes);
            }
        }
        // 数据 copy 之后需要更新列开始在 buffer 中的位置
        // 在更新的时候也有可能是跳过一些需要丢弃的符号，主要用于使用文本边界的时候，跳过双引号
        startedColumnPosition = buffer.position() + 1;
    }

    private void checkDataLength() throws IOException {
        // 在没有数据的时候，可能有以下几种情况
        // - 在解析中 buffer 耗尽的时候
        // 所以需要把之前已经解析的数据备份存档
        updateCurrentColumnBuffer();
        hasMoreData = this.getMoreData();
        if (hasMoreData) {
            // 重新读取之后，列开始只能是从 0 开始
            startedColumnPosition = 0;
        }
    }

    /**
     * 唯一要做的就是：获取一次更多的数据
     * @return
     * @throws IOException
     */
    protected abstract boolean getMoreData() throws IOException;

    private class Letters {
        public static final byte LF = '\n';

        public static final byte CR = '\r';

        public static final byte COMMA = ',';

        public static final byte TEXT_QUALIFIER = '"';
    }
}
