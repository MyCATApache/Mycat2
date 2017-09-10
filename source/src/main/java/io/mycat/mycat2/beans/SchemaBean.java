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
 */
package io.mycat.mycat2.beans;

import java.util.List;

/**
 * Mycat Logic Schema
 *
 * @author wuzhihui
 */

public class SchemaBean {
	public enum SchemaType {
		// 所有表在一个MySQL Server上（但不分片），
		DB_IN_ONE_SERVER,
		// 所有表在不同的MySQL Server上（但不分片），
		DB_IN_MULTI_SERVER,
		// 只使用基于SQL注解的路由模式（高性能但手工指定）
		ANNOTATION_ROUTE,
		// 使用SQL解析的方式去判断路由
		SQL_PARSE_ROUTE
	}

	public String name;
	public SchemaType type;
	private DNBean defaultDN;
	private List<TableDefBean> tables;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public SchemaType getType() {
		return type;
	}

	public void setType(SchemaType type) {
		this.type = type;
	}

	public DNBean getDefaultDN() {
		return defaultDN;
	}

	public void setDefaultDN(DNBean defaultDN) {
		this.defaultDN = defaultDN;
	}

	public List<TableDefBean> getTables() {
		return tables;
	}

	public void setTables(List<TableDefBean> tables) {
		this.tables = tables;
	}

	@Override
	public String toString() {
		return "SchemaBean [name=" + name + ", type=" + type + ", defaultDN=" + defaultDN
				+ ", tables=" + tables + "]";
	}

}
