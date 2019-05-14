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
package io.mycat.beans.mysql.charset;

/**
 * @author jamie12221
 * @date 2019-05-05 16:22
 *
 * 字符集元数据
 **/
public class MySQLCollation {

  String collatioNname;
  String charsetName;
  int id;
  boolean isDefault;
  boolean compiled;
  int sortLen;

  public String getCollatioNname() {
    return collatioNname;
  }

  public void setCollatioNname(String collatioNname) {
    this.collatioNname = collatioNname;
  }

  public String getCharsetName() {
    return charsetName;
  }

  public void setCharsetName(String charsetName) {
    this.charsetName = charsetName;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public boolean isDefault() {
    return isDefault;
  }

  public void setDefault(boolean aDefault) {
    isDefault = aDefault;
  }

  public boolean isCompiled() {
    return compiled;
  }

  public void setCompiled(boolean compiled) {
    this.compiled = compiled;
  }

  public int getSortLen() {
    return sortLen;
  }

  public void setSortLen(int sortLen) {
    this.sortLen = sortLen;
  }
}
