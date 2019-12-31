  
/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans.mycat;

import io.mycat.replica.ReplicaSwitchType;

/**
 * @author jamie12221 date 2019-05-07 11:29 抽象Mycat集群管理类,它的子类可能是mycat实现的mycat
 * 集群管理以及jdbc实现的集群管理,前者在proxy中运行
 **/
public interface MycatReplica {

  boolean switchDataSourceIfNeed();

  String getName();

  ReplicaSwitchType getSwitchType();
}