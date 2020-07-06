package io.mycat.sqlhandler;

import lombok.Getter;

@Getter
public enum ExecuteCode {
    NOT_PERFORMED(0),//mycat状态码-未执行完,等待下次请求继续执行
    PERFORMED(200),//mycat状态码-执行正常
    PROXY_ERROR(300),//mycat状态码-代理错误
    CLIENT_ERROR(400),//mycat状态码-客户端错误
    SERVER_ERROR(500)//mycat状态码-服务端错误
    ;

//    int CODE_0 = 0;//mycat状态码-未执行
//    int CODE_100 = 100;//mycat状态码-未执行完,等待下次请求继续执行
//    int CODE_200 = 200;//mycat状态码-执行正常
//    int CODE_300 = 300;//mycat状态码-代理错误
//    int CODE_400 = 400;//mycat状态码-客户端错误
//    int CODE_500 = 500;//mycat状态码-服务端错误


    final int value;

    ExecuteCode(int value) {
        this.value = value;
    }
}