package io.mycat;

import com.rits.cloning.Cloner;
import io.mycat.config.ShardingQueryRootConfig;
import io.mycat.config.ShardingTableConfig;

import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;

public class TemplateFileConfigProviderImpl extends FileConfigProvider {
    @Override
    public void init(Class rootClass, Map<String, String> config) throws Exception {
        super.init(rootClass, config);
    }

    @Override
    public void fetchConfig(String url) throws Exception {
        super.fetchConfig(url);
        Optional.ofNullable(this.config).map(m -> m.getMetadata()).map(m -> m.getSchemas()).ifPresent(m -> {
            Map.Entry<String, ShardingTableConfig> mt_$ = null;
            Map<String, ShardingTableConfig> shadingTables = null;
            OUTER:
            for (ShardingQueryRootConfig.LogicSchemaConfig i : m) {
                if (i.getShadingTables() != null) {
                    shadingTables = i.getShadingTables();
                    for (Map.Entry<String, ShardingTableConfig> s :shadingTables.entrySet()) {
                        if (s.getKey().startsWith("mt_")) {
                            mt_$ = s;
                            break OUTER;
                        }
                    }
                }
            }
            if (mt_$ != null) {
                LocalDate now = LocalDate.now();
                LocalDate start = now.minusYears(1);
                LocalDate iter = start;
                LocalDate end = now.plusYears(1);

                for (; ; ) {
                    String year = String.format("%04d", iter.getYear());
                    String month = String.format("%02d", iter.getMonthValue());
                    String dayOfMonth = String.format("%02d", iter.getDayOfMonth());
                    String key = "mt_"+year + month + dayOfMonth;
                    ShardingTableConfig shardingTableConfig = Cloner.standard().deepClone(mt_$.getValue());
                    for (ShardingQueryRootConfig.BackEndTableInfoConfig dataNode : shardingTableConfig.getDataNodes()) {
                        dataNode.setTableName(key);
                    }
                    shadingTables.put(key,shardingTableConfig);
                    iter = iter.plusDays(1);
                    if (iter.equals(end)){
                        break;
                    }
                }
            }
        });
    }
}