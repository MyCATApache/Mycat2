/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mycat.plug.loadBalance;

import java.util.List;

/**
 * @author jamie12221
 *  date 2019-05-13 14:45 负载均衡算法接口
 **/
@FunctionalInterface
public interface LoadBalanceStrategy {

    /**
     * @param info 全局信息
     * @param entityList 可选列表
     */
    LoadBalanceElement select(LoadBalanceInfo info, List<LoadBalanceElement> entityList);
}
