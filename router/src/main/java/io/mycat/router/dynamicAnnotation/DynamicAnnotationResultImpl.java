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
package io.mycat.router.dynamicAnnotation;

import io.mycat.MycatExpection;
import io.mycat.config.route.AnnotationType;
import io.mycat.router.DynamicAnnotationResult;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * @author jamie12221
 *  date 2019-05-03 23:50
 **/
public class DynamicAnnotationResultImpl implements DynamicAnnotationResult {

  //final HashMap<String, AnnotationType> lastAnnotationTypeMap = new HashMap<>();
  final HashMap<String, String> result = new HashMap<>();
  private static final BiConsumer<AnnotationType, List<String>> clearAction = (k, v) -> v.clear();
  public static DynamicAnnotationResultImpl EMPTY = new DynamicAnnotationResultImpl();
  String sql;

  public String getSql() {
    return sql;
  }

  public DynamicAnnotationResultImpl() {
  }

  private static void accept(AnnotationType k, List<String> v) {
    v.clear();
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  @Override
  public String getSQL() {
    return sql;
  }

  public void clear() {
    result.clear();
    checkDup.setValue(null);
    setSql(null);
  }

  public String get(String key) {
    return result.get(key);
  }

  public void put(String key, String value, AnnotationType type) {
    checkDup.setValue(value);
    result.compute(key, checkDup);
    checkDup.setValue(null);
  }


  private final CheckDup checkDup = new CheckDup();

  private class CheckDup implements BiFunction<String, String, String> {

    private String value;

    public CheckDup() {

    }

    void setValue(String value) {
      this.value = value;
    }

    @Override
    public String apply(String s, String s2) {
      if (s2 != null) {
        throw new MycatExpection("duplicated key!");
      }
      return value;
    }
  }
}
