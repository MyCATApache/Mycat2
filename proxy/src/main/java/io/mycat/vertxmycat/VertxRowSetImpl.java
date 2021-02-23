/*
 * Copyright (C) 2017 Julien Viet
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.mycat.vertxmycat;

import io.vertx.mysqlclient.MySQLClient;
import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import io.vertx.sqlclient.impl.SqlResultBase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class VertxRowSetImpl<R>  implements RowSet<R> {

  public List<R> list = new LinkedList<>();
   List<ColumnDescriptor> columnDescriptor;
  long affectRow;
  long lastInsertId;

  public VertxRowSetImpl() {

  }

  @Override
  public int rowCount() {
    return (int)affectRow;
  }

  @Override
  public List<String> columnsNames() {
    return  this.columnDescriptor.stream().map(i->i.name()).collect(Collectors.toList());
  }

  @Override
  public List<ColumnDescriptor> columnDescriptors() {
    return this.columnDescriptor;
  }

  @Override
  public int size() {
    return list.size();
  }

  @Override
  public <V> V property(PropertyKind<V> propertyKind) {
    if (propertyKind == MySQLClient.LAST_INSERTED_ID){
      Long lastInsertId = this.lastInsertId;
      return (V)lastInsertId;
    }
    return null;
  }

  @Override
  public RowSet<R> value() {
    return this;
  }

  @Override
  public RowIterator<R> iterator() {
    Iterator<R> i = list.iterator();
    return new RowIterator<R>() {
      @Override
      public boolean hasNext() {
        return i.hasNext();
      }
      @Override
      public R next() {
        return i.next();
      }
    };
  }

  @Override
  public VertxRowSetImpl<R> next() {
    return null;
  }

  public void setAffectRow(long affectRow) {
    this.affectRow = affectRow;
  }

  public void setLastInsertId(long lastInsertId) {
    this.lastInsertId = lastInsertId;
  }

  public List<ColumnDescriptor> getColumnDescriptor() {
    return columnDescriptor;
  }

  public void setColumnDescriptor(List<ColumnDescriptor> columnDescriptor) {
    this.columnDescriptor = columnDescriptor;
  }
}
