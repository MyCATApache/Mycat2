/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SensitiveRemover {
    public static void main(String[] args) {
        String sql = "SELECT\n" +
                "\tch.id,\n" +
                "\tch.channel_id,\n" +
                "\tch. NAME,\n" +
                "\tch.notice,\n" +
                "\tch.source_full_length,\n" +
                "\tch.source_segment,\n" +
                "\tch.call_out,\n" +
                "\tch.source_type,\n" +
                "\tch.to_cmcc,\n" +
                "\tch.to_unicom,\n" +
                "\tch.to_telecom,\n" +
                "\tch.to_unknown,\n" +
                "\tch.msgid_type,\n" +
                "\tch.have_report,\n" +
                "\tch.have_mo,\n" +
                "\tch.success_rate,\n" +
                "\tch.size_max,\n" +
                "\tch.size_first,\n" +
                "\tch.size_charge,\n" +
                "\tch.cover_key,\n" +
                "\tch.submit_begin,\n" +
                "\tch.submit_end,\n" +
                "\tch.auto_ext_src,\n" +
                "\tch.user_ext_src,\n" +
                "\tch.channel_status,\n" +
                "\tch.baobei_model,\n" +
                "\tch.filter_global_black,\n" +
                "\tch.to_intl,\n" +
                "\tch.gw_type,\n" +
                "\tch.svc_addr,\n" +
                "\tch.svc_port,\n" +
                "\tch.account,\n" +
                "\tch. PASSWORD,\n" +
                "\tch.link_max,\n" +
                "\tch.link_speed,\n" +
                "\tch.first_msg_charge_len,\n" +
                "\tch.subseq_msg_charge_len,\n" +
                "\tch.service_code,\n" +
                "\tch.enterprise_code,\n" +
                "\tch.extras,\n" +
                "\tch.short_num,\n" +
                "\tch.variant,\n" +
                "\tch.backup_channel_id,\n" +
                "\tch.auto_extract_signs,\n" +
                "\tch.channel_price,\n" +
                "\tch.model,\n" +
                "\tch.alarm_code,\n" +
                "\tch.submit_alarm_code,\n" +
                "\tch.logic_model,\n" +
                "\tch.mo_match,\n" +
                "\tch.baobei_before_pj_ext_src,\n" +
                "\tch.is_month_limit,\n" +
                "\tch.month_limit_count,\n" +
                "\t(\n" +
                "\t\tCASE\n" +
                "\t\tWHEN co.count IS NULL THEN\n" +
                "\t\t\t0\n" +
                "\t\tELSE\n" +
                "\t\t\tco.count\n" +
                "\t\tEND\n" +
                "\t) AS productNumber,\n" +
                "\tch.platform_flag,\n" +
                "\tch.all_platform_used,\n" +
                "\tb.head,\n" +
                "\tb.supplier_key,\n" +
                "\tch.redis_flag\n" +
                "FROM\n" +
                "\tsms_core.args_channel ch\n" +
                "LEFT JOIN (\n" +
                "\tSELECT\n" +
                "\t\tchannel_id,\n" +
                "\t\tCOUNT(1) AS count\n" +
                "\tFROM\n" +
                "\t\t(\n" +
                "\t\t\tSELECT\n" +
                "\t\t\t\tcmcc_channel_id AS channel_id,\n" +
                "\t\t\t\tid\n" +
                "\t\t\tFROM\n" +
                "\t\t\t\tsms_core.fors_product\n" +
                "\t\t\tUNION\n" +
                "\t\t\t\tSELECT\n" +
                "\t\t\t\t\tunicom_channel_id AS channel_id,\n" +
                "\t\t\t\t\tid\n" +
                "\t\t\t\tFROM\n" +
                "\t\t\t\t\tsms_core.fors_product\n" +
                "\t\t\t\tUNION\n" +
                "\t\t\t\t\tSELECT\n" +
                "\t\t\t\t\t\ttelecom_channel_id AS channel_id,\n" +
                "\t\t\t\t\t\tid\n" +
                "\t\t\t\t\tFROM\n" +
                "\t\t\t\t\t\tsms_core.fors_product\n" +
                "\t\t) mm\n" +
                "\tGROUP BY\n" +
                "\t\tchannel_id\n" +
                ") co ON ch.channel_id = co.channel_id\n" +
                "LEFT JOIN args_channel_ext a ON ch.channel_id = a.channel_id\n" +
                "LEFT JOIN base_channel_supplier b ON b.id = a.supplier_id\n" +
                "WHERE\n" +
                "\t1 = 1\n" +
                "ORDER BY\n" +
                "\tch.channel_id DESC\n" +
                "LIMIT 0,\n" +
                " 50";
        System.out.println(sql);
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        AtomicInteger tableAcc = new AtomicInteger();
        Map<String, String> tableMapping = new HashMap<>();
        Map<String, String> columnMapping = new HashMap<>();
        AtomicInteger columnAcc = new AtomicInteger();
        AtomicInteger propertyAcc = new AtomicInteger();
        AtomicInteger aliasAcc = new AtomicInteger();
        sqlStatement.accept(new MySqlASTVisitorAdapter() {
            @Override
            public boolean visit(SQLExprTableSource x) {
                if (x.getTableName() != null) {
                    String absent = tableMapping.computeIfAbsent(x.getTableName(), s -> {
                        String newTableName = "t_" + tableAcc.getAndIncrement();
                        return newTableName;
                    });

                    tableMapping.put(x.getTableName(), absent);
                    x.setSimpleName(absent);
                }
                return super.visit(x);
            }

            @Override
            public boolean visit(SQLJoinTableSource x) {
                String alias = x.getAlias();
                if (alias != null) {
                    x.setAlias("a_" + aliasAcc.getAndIncrement());
                }
                return super.visit(x);
            }

            @Override
            public boolean visit(SQLPropertyExpr x) {
                String newColumnName = "p_" + propertyAcc.getAndIncrement();
                x.setName(newColumnName);
                return super.visit(x);
            }

            @Override
            public boolean visit(SQLSelectItem x) {
                String alias = x.getAlias();
                if (alias != null) {
                    x.setAlias("a_" + aliasAcc.getAndIncrement());
                }
                return super.visit(x);
            }

            @Override
            public boolean visit(SQLIdentifierExpr x) {
                String newColumnName = "i_" + columnAcc.getAndIncrement();
                x.setName(newColumnName);
                return super.visit(x);
            }
        });
        System.out.println(sqlStatement);

    }
}