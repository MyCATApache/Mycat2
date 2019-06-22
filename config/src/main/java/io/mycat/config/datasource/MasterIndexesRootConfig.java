/**
 * Copyright (C) <2019>  <chen junwen,gaozhiwen>
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


package io.mycat.config.datasource;


import io.mycat.config.ConfigurableRoot;
import java.util.Map;

/**
 * Desc: 加载replica-index.yml文件配置
 *
 * date: 10/09/2017
 * @author: gaozhiwen
 */
public class MasterIndexesRootConfig implements ConfigurableRoot {

    private Map<String, String> masterIndexes;

    public Map<String, String> getMasterIndexes() {
        return masterIndexes;
    }

    public void setMasterIndexes(Map<String, String> masterIndexes) {
        this.masterIndexes = masterIndexes;
    }
}
