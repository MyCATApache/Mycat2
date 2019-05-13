/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy;

import io.mycat.proxy.session.Session;

/**
 * 在Reactor线程中传递局部变量
 * @author wuzhihui
 *
 * 此环境对象指示当前读取事件回调的session
 * * @author chen junwen
 */
public class ReactorEnv {
    public Session getCurSession() {
        return curSession;
    }

    public void setCurSession(Session curSession) {
        this.curSession = curSession;
    }

    private Session curSession;

}
