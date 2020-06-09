package io.mycat.hbt2.nosql;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * nosql响应
 * @author wangzihaogithub
 * 2020年6月9日 00:58:43
 */
public interface NosqlResponse {
    /**
     * 默认编码,比如key的编码, value的编码.
     */
    Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    /**
     * 获取错误码
     * @return 错误码. 0=成功
     */
    int getErrorCode();
    /**
     * 获取错误信息 (具体供应商的实现错误信息, 越详细越好)
     * @return 错误信息
     */
    String getErrorMessage();

    int CODE_SUCCESS = 0;

}
