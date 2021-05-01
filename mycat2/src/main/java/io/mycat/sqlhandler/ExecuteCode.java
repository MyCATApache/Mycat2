/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
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