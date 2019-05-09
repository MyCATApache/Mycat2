package io.mycat.config.route;

import java.util.List;

/**
 * @author jamie12221
 * @date 2019-05-03 14:22
 **/
public class ShardingRule {
  String column;
  List<String> equalAnnotations;
  List<String> rangeAnnotations;
  String equalKey;
  String rangeStart;
  String rangeEnd;


  public String getEqualKey() {
    return equalKey;
  }

  public void setEqualKey(String equalKey) {
    this.equalKey = equalKey;
  }

  public String getRangeStart() {
    return rangeStart;
  }

  public void setRangeStart(String rangeStart) {
    this.rangeStart = rangeStart;
  }

  public String getRangeEnd() {
    return rangeEnd;
  }

  public void setRangeEnd(String rangeEnd) {
    this.rangeEnd = rangeEnd;
  }

  public String getColumn() {
    return column;
  }

  public List<String> getEqualAnnotations() {
    return equalAnnotations;
  }

  public void setEqualAnnotations(List<String> equalAnnotations) {
    this.equalAnnotations = equalAnnotations;
  }

  public List<String> getRangeAnnotations() {
    return rangeAnnotations;
  }

  public void setRangeAnnotations(List<String> rangeAnnotations) {
    this.rangeAnnotations = rangeAnnotations;
  }


  public void setColumn(String column) {
    this.column = column;
  }


}
