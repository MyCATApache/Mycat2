package io.mycat.hbt2.nosql;

/**
 * nosql响应
 * @author wangzihaogithub
 * 2020年6月9日 00:58:43
 */
public interface NosqlResponse {
    /**
     * 获取错误码
     * @return 错误码. 0=成功
     */
    int getErrorCode();
    String getErrorMessage();

    int CODE_SUCCESS = 0;

}
