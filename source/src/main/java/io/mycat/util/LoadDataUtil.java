package io.mycat.util;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.CurSQLState;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.LoadDataState;
import io.mycat.proxy.ProxyBuffer;

public class LoadDataUtil {
    private static Logger logger = LoggerFactory.getLogger(LoadDataUtil.class);

    private static final int FLAGLENGTH = 4;

    /**
     * 进行结束符的读取
     *
     * @param curBuffer buffer数组信息
     */
    public static void readOverByte(MycatSession session, ProxyBuffer curBuffer) {
        byte[] overFlag = getOverFlag(session);
        // 获取当前buffer的最后
        ByteBuffer buffer = curBuffer.getBuffer();

        // 如果数据的长度超过了，结束符的长度，可直接提取结束符
        if (buffer.position() >= FLAGLENGTH) {
            int opts = curBuffer.writeIndex;
            buffer.position(opts - FLAGLENGTH);
            buffer.get(overFlag, 0, FLAGLENGTH);
            buffer.position(opts);
        }
        // 如果小于结束符，说明需要进行两个byte数组的合并
        else {
            int opts = curBuffer.writeIndex;
            // 计算放入的位置
            int moveSize = FLAGLENGTH - opts;
            int index = 0;
            // 进行数组的移动,以让出空间进行放入新的数据
            for (int i = FLAGLENGTH - moveSize; i < FLAGLENGTH; i++) {
                overFlag[index] = overFlag[i];
                index++;
            }
            // 读取数据
            buffer.position(0);
            buffer.get(overFlag, moveSize, opts);
            buffer.position(opts);
        }

    }

    /**
     * 进行结束符的检查,
     * <p>
     * 数据的结束符为0,0,0,包序，即可以验证读取到3个连续0，即为结束
     *
     * @return
     */
    public static boolean checkOver(MycatSession session) {
        byte[] overFlag = getOverFlag(session);
        for (int i = 0; i < overFlag.length - 1; i++) {
            if (overFlag[i] != 0) {
                return false;
            }
        }
        return true;
    }

    /*获取结束flag标识的数组*/
    public static byte[] getOverFlag(MycatSession session) {
        byte[] overFlag = (byte[]) session.curSQLSate.get(CurSQLState.LOAD_OVER_FLAG_ARRAY);
        if (overFlag != null) {
            return overFlag;
        }
        overFlag = new byte[FLAGLENGTH];
        session.curSQLSate.set(CurSQLState.LOAD_OVER_FLAG_ARRAY, overFlag);
        return overFlag;
    }

    public static void change2(MycatSession session, LoadDataState loadDataState) {
        logger.info("from {} to {}", session.loadDataStateMachine, loadDataState);
        session.loadDataStateMachine = loadDataState;
    }
}
