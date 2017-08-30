/*
 * Copyright (c) 2016, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */package io.mycat.mycat2;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import io.mycat.mycat2.beans.MySQLBean;
import io.mycat.mycat2.beans.MySQLDataSource;
import io.mycat.mycat2.beans.MySQLRepBean;

/**
 * 表示一组DataHost的数据复制关系，如主从，主主
 * 
 * @author wuzhihui
 *
 */
public class MySQLReplicatSet {

	public static final int M_S_REP = 0;
	public static final int M_M_REP = 1;

	private int repType;
	private final String name;
	private int switchType = 0;
	private int writeIndex = 0;
	private MySQLDataSource[] dhSources;

	public MySQLReplicatSet(final MySQLRepBean repBean, final int curWriteIndex) {
		this.name = repBean.getName();
		this.writeIndex = curWriteIndex;
		final List<MySQLBean> mysqlBeans = repBean.getMysqls();
		this.repType = repBean.getType();
		this.switchType = repBean.getSwitchType();
		dhSources = new MySQLDataSource[mysqlBeans.size()];
		for (int i = 0; i < dhSources.length; i++) {
			dhSources[i] = new MySQLDataSource(mysqlBeans.get(i), writeIndex != i);
			dhSources[i].initSource();
		}
	}

	public String getName() {
		return name;
	}

	public int getRepType() {
		return repType;
	}

	/**
	 * 是否支持主宕机后的自动切换能力
	 * 
	 * @return
	 */

	public boolean supportAutoSwitch() {

		return switchType != 0;
	}

	/**
	 * 得到当前用于写的DHSource
	 * 
	 * @return DHSource
	 */
	public MySQLDataSource getCurWriteDH() {
		return dhSources[writeIndex];
	}

	/**
	 * 得到当前用于读的DHSource（负载均衡模式，如果支持）
	 * 
	 * @return DHSource
	 */
	public MySQLDataSource getLBReadDH() {
		return dhSources[ThreadLocalRandom.current().nextInt() / dhSources.length];
	}

	@Override
	public String toString() {
		final StringBuilder sbuf = new StringBuilder("MySQLReplicatSet[").append("name=").append(name).append(',')
				.append("repType=").append(repType).append(',').append("switchType=").append(switchType).append(',')
				.append("writeIndex=").append(writeIndex).append(',').append("dhSources=").append('[');
		for (int i = 0, size = dhSources.length; i < size; ++i) {
			final MySQLDataSource ds = dhSources[i];
			sbuf.append(i == 0 ? "" : ',').append(ds);
		}
		sbuf.append(']').append(']');
		return (sbuf.toString());
	}

}
