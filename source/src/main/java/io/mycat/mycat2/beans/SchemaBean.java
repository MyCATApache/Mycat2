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
		DBInOneServer,
		// 所有表在不同的MySQL Server上（但不分片），
		DBINMultiServer,
		// 只使用基于SQL注解的路由模式（高性能但手工指定）
		AnnotateRoute,
		// 使用SQL解析的方式去判断路由
		SQLParseRoute
	}

	public String name;
	public SchemaType type;
	private DNBean defaultDN;
	/**
	 * 是否非分片的Schema，意味著沒有任何分片表的Schema
	 */
	private final boolean normalSchema;
	private List<TableDefBean> tableDefBeans;

	public SchemaBean(String name, DNBean defaultDN, boolean normalSchema, List<TableDefBean> tableDefBeans) {
		super();
		this.name = name;
		this.defaultDN = defaultDN;
		this.normalSchema = normalSchema;
		this.tableDefBeans = tableDefBeans;
	}

	public boolean isNormalSchema() {
		return normalSchema;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public DNBean getDefaultDN() {
		return defaultDN;
	}

	public void setDefaultDN(DNBean defaultDN) {
		this.defaultDN = defaultDN;
	}

	public void setTableDefBeans(List<TableDefBean> tableDefBeans) {
		this.tableDefBeans = tableDefBeans;
	}

	public List<TableDefBean> getTableDefBeans() {
		return tableDefBeans;
	}

	@Override
	public String toString() {
		return "SchemaBean [name=" + name + ", defaultDN=" + defaultDN + ", normalSchema=" + normalSchema
				+ ", tableDefBeans=" + tableDefBeans + "]";
	}

}
