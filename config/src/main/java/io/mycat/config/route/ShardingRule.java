package io.mycat.config.route;

import java.util.List;

/**
 * @author jamie12221
 *  date 2019-05-03 14:22
 **/
public class ShardingRule {
  String column;
  List<String> equalAnnotations;
  List<String> rangeAnnotations;
  String equalKey;
  String rangeStartKey;
  String rangeEndKey;


  public String getEqualKeys() {
    return equalKey;
  }

  public void setEqualKey(String equalKey) {
    this.equalKey = equalKey;
  }

  public String getRangeStartKey() {
    return rangeStartKey;
  }

  public void setRangeStartKey(String rangeStartKey) {
    this.rangeStartKey = rangeStartKey;
  }

  public String getRangeEndKey() {
    return rangeEndKey;
  }

  public void setRangeEndKey(String rangeEndKey) {
    this.rangeEndKey = rangeEndKey;
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
