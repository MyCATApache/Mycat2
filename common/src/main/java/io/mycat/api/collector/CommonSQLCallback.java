package io.mycat.api.collector;

import java.util.List;
import java.util.Map;

/**
 * @author jamie12221
 * date 2019-05-22 23:18
 * simple result set callback
 **/
public interface CommonSQLCallback {

    List<String> getSqls();

    void process(List<List<Map<String, Object>> >resultSetList);

    void onException(Exception e);
}