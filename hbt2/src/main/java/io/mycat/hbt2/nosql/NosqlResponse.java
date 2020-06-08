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
    /**
     * 获取错误信息 (具体供应商的实现错误信息, 越详细越好)
     * @return 错误信息
     */
    String getErrorMessage();

    int CODE_SUCCESS = 0;

}
