/**
 * Copyright (C) <2019>  <yannan>
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
package io.mycat;

/**
 * @author jamie12221
 * @date 2019-04-30 16:24
 * mysql session占用类型
 **/
public enum MySQLSessionMonopolizeType {
  NONE,
  TRANSACTION,
  LOAD_DATA,
  PREPARE_STATEMENT_EXECUTE,
  CURSOR_EXISTS,
  ;
}
