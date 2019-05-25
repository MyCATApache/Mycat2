package io.mycat.beans.mysql;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author jamie12221
 * @date 2019-05-23 17:22
 **/
public class MySQLVariables {

  final Map<String, String> variables = new HashMap<>();

  public MySQLVariables() {

    variables.put("tx_isolation", "REPEATABLE-READ");//
    variables.put("transaction_isolation", "REPEATABLE-READ");//

    variables.put("auto_increment_increment", "1");
    variables.put("net_write_timeout", "60");
    variables.put("local.character_set_results", "-1");

    variables.put("character_set_client", "utf8");//
    variables.put("character_set_connection", "utf8");//
    variables.put("character_set_results", "utf8");//
    variables.put("character_set_server", "utf8");//
    variables.put("init_connect", "");//
    variables.put("interactive_timeout", "172800");//
    variables.put("lower_case_table_names", "1");
    variables.put("max_allowed_packet", "16777216");
    variables.put("net_buffer_length", "8192");
    variables.put("net_write_timeout", "60");
    variables.put("query_cache_size", "0");
    variables.put("query_cache_type", "OFF");
    variables.put("sql_mode", "STRICT_TRANS_TABLES");//
    variables.put("system_time_zone", "CST");//
    variables.put("time_zone", "SYSTEM");//
    variables.put("lower_case_table_names", "1");
    variables.put("wait_timeout", "172800");//

    variables.put("character_set_system", "utf8");//
    variables.put("collation_server", "");//
    variables.put("performance_schema", "");//
  }

  public Set<Entry<String, String>> entries() {
    return variables.entrySet();
  }

}
