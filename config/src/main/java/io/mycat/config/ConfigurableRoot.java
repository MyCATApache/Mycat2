/**
 * Copyright (C) <2019>  <gaozhiwen>
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
package io.mycat.config;

/**
 * Desc:
 *
 * date: 16/09/2017
 *
 * @author: gaozhiwen
 */
public abstract class ConfigurableRoot implements ConfigurableNode {

  String filePath;
  volatile int version;
  ConfigFile type;

  public void setFilePath(String path) {
    filePath = path;
  }

  public int getVersion() {
    return version;
  }

  public ConfigFile getType() {
    return type;
  }


  public void setType(ConfigFile type) {
    this.type = type;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setVersion(int version) {
    this.version = version;
  }
}
