package io.mycat.beans.mysql;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

/**
 * @author jamie12221
 *  date 2019-05-23 17:22
 **/
public class MySQLVariables {
  long net_buffer_length = 8192;
  long interactive_timeout = 172800;
  long query_cache_size = 0;
  String performance_schema = "";
  private Map<String, String> variables;
  long net_write_timeout = 0;
  String character_set_connection = "utf8";
  int max_allowed_packet = 16777216;
  String lower_case_table_names = "1";
  String collation_server = "";
  String tx_isolation = "REPEATABLE-READ";
  String system_time_zone = "CST";
  long wait_timeout = 172800;
  String time_zone = "SYSTEM";
  String character_set_server = "utf8";
  long auto_increment_increment = 1;
  String character_set_client = "utf8";
  String sql_mode = "STRICT_TRANS_TABLES";
  String character_set_results = "utf8";
  String transaction_isolation = "REPEATABLE-READ";
  String character_set_system = "utf8";
  String query_cache_type = "OFF";
  String init_connect = "";
  int local_character_set_results = -1;

  public MySQLVariables(Map<String, String> variables) {
    tx_isolation = variables.getOrDefault("tx_isolation", "REPEATABLE-READ");
    transaction_isolation = variables.getOrDefault("transaction_isolation", "REPEATABLE-READ");
    auto_increment_increment = Long
        .parseLong(variables.getOrDefault("auto_increment_increment", "1"));
    net_write_timeout = Integer.parseInt(variables.getOrDefault("net_write_timeout", "60"));
    local_character_set_results = Integer
        .parseInt(variables.getOrDefault("local.character_set_results", "-1"));
    character_set_client = variables.getOrDefault("character_set_client", "utf8");
    character_set_connection = variables.getOrDefault("character_set_connection", "utf8");
    character_set_results = variables.getOrDefault("character_set_results", "utf8");
    character_set_server = variables.getOrDefault("character_set_server", "utf8");
    init_connect = variables.getOrDefault("init_connect", "");
    interactive_timeout = Long.parseLong(variables.getOrDefault("interactive_timeout", "172800"));
    max_allowed_packet = Integer.parseInt(variables.getOrDefault("max_allowed_packet", "524288000"));
    net_buffer_length = Long.parseLong(variables.getOrDefault("net_buffer_length", "8192"));
    query_cache_size = Long.parseLong(variables.getOrDefault("query_cache_size", "0"));
    query_cache_type = variables.getOrDefault("query_cache_size", "OFF");
    sql_mode = variables.getOrDefault("sql_mode", "STRICT_TRANS_TABLES");
    system_time_zone = variables.getOrDefault("system_time_zone", "CST");
    time_zone = variables.getOrDefault("time_zone", "SYSTEM");
    lower_case_table_names = variables.getOrDefault("lower_case_table_names", "1");
    wait_timeout = Long.parseLong(variables.getOrDefault("wait_timeout", "172800"));
    character_set_system = variables.getOrDefault("character_set_system", "utf8");
    collation_server = variables.getOrDefault("collation_server", "");
    performance_schema = variables.getOrDefault("performance_schema", "");
    this.variables = variables;
  }

  public void flash() {
    HashMap<String, String> variables = new HashMap<>();
    variables.put("tx_isolation", Objects.toString(tx_isolation));//
    variables.put("transaction_isolation", transaction_isolation);//

    variables.put("auto_increment_increment", Objects.toString(auto_increment_increment));
    variables.put("net_write_timeout", Objects.toString(net_write_timeout));
    variables.put("local.character_set_results", Objects.toString(local_character_set_results));

    variables.put("character_set_client", character_set_client);//
    variables.put("character_set_connection", character_set_connection);//
    variables.put("character_set_results", character_set_results);//
    variables.put("character_set_server", character_set_server);//
    variables.put("init_connect", init_connect);//
    variables.put("interactive_timeout", Objects.toString(interactive_timeout));//
    variables.put("max_allowed_packet", Objects.toString(max_allowed_packet));
    variables.put("net_buffer_length", Objects.toString(net_buffer_length));
    variables.put("query_cache_size", Objects.toString(query_cache_size));
    variables.put("query_cache_type", Objects.toString(query_cache_type));
    variables.put("sql_mode", Objects.toString(sql_mode));//
    variables.put("system_time_zone", Objects.toString(system_time_zone));//
    variables.put("time_zone", Objects.toString(time_zone));//
    variables.put("lower_case_table_names", Objects.toString(lower_case_table_names));
    variables.put("wait_timeout", Objects.toString(wait_timeout));//

    variables.put("character_set_system", Objects.toString(character_set_system));//
    variables.put("collation_server", Objects.toString(collation_server));//
    variables.put("performance_schema", Objects.toString(performance_schema));//

    this.variables = variables;
  }

  public Map<String, String> toStringMap() {
    return Collections.unmodifiableMap(this.variables);
  }
  public Set<Entry<String, String>> entries() {
    return variables.entrySet();
  }

  public long getNetBufferLength() {
    return net_buffer_length;
  }

  public long getInteractiveTimeout() {
    return interactive_timeout;
  }

  public long getQueryCacheSize() {
    return query_cache_size;
  }

  public String getPerformanceSchema() {
    return performance_schema;
  }

  public long getNetWriteTimeout() {
    return net_write_timeout;
  }

  public String getCharacterSetConnection() {
    return character_set_connection;
  }

  public int getMaxAllowedPacket() {
    return max_allowed_packet;
  }

  public String getLowerCaseTableNames() {
    return lower_case_table_names;
  }

  public String getCollationServer() {
    return collation_server;
  }

  public String getTxIsolation() {
    return tx_isolation;
  }

  public String getSystemTimeZone() {
    return system_time_zone;
  }

  public long getWaitTimeout() {
    return wait_timeout;
  }

  public String getTimeZone() {
    return time_zone;
  }

  public String getCharacterSetServer() {
    return character_set_server;
  }

  public long getAutoIncrementIncrement() {
    return auto_increment_increment;
  }

  public String getCharacterSetClient() {
    return character_set_client;
  }

  public String getSqlMode() {
    return sql_mode;
  }

  public String getCharacterSetResults() {
    return character_set_results;
  }

  public String getTransactionIsolation() {
    return transaction_isolation;
  }

  public String getCharacterSetSystem() {
    return character_set_system;
  }

  public String getQueryCacheType() {
    return query_cache_type;
  }

  public String getInitConnect() {
    return init_connect;
  }

  public int getLocalCharacterSetResults() {
    return local_character_set_results;
  }
}
