package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLCreateSequenceStatement;
import io.mycat.MycatDataContext;
import io.mycat.config.MycatRouterConfigOps;
import io.mycat.config.SequenceConfig;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;

import java.util.Optional;

public class CreateSequenceHandler extends AbstractSQLHandler<SQLCreateSequenceStatement> {
    @Override
    protected void onExecute(SQLRequest<SQLCreateSequenceStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        SQLCreateSequenceStatement requestAst = request.getAst();

        SequenceConfig config = new SequenceConfig();

        Optional.ofNullable(requestAst.getName()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setName(text);
        });
        Optional.ofNullable(requestAst.getIncrementBy()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setIncrementBy(text);
        });
        Optional.ofNullable(requestAst.getMinValue()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setMinValue(text);
        });
        Optional.ofNullable(requestAst.getMaxValue()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setMaxValue(text);
        });
        Optional.ofNullable(requestAst.isNoMaxValue()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setNoMinValue( Boolean.parseBoolean(text));
        });
        Optional.ofNullable(requestAst.isNoMinValue()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setNoMinValue(Boolean.parseBoolean(text));
        });
        Optional.ofNullable(requestAst.getWithCache()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setWithCache(Boolean.parseBoolean(text));
        });
        Optional.ofNullable(requestAst.getCycle()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setCycle(Boolean.parseBoolean(text));
        });
        Optional.ofNullable(requestAst.getCache()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setCache(Boolean.parseBoolean(text));
        });
        Optional.ofNullable(requestAst.getCacheValue()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setCacheValue(Long.parseLong(text));
        });
        Optional.ofNullable(requestAst.getOrder()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setOrder(Boolean.parseBoolean(text));
        });
        Optional.ofNullable(requestAst.isSimple()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setSimple(Boolean.parseBoolean(text));
        });
        Optional.ofNullable(requestAst.isGroup()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setGroup(Boolean.parseBoolean(text));
        });
        Optional.ofNullable(requestAst.isTime()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setTime(Boolean.parseBoolean(text));
        });
        Optional.ofNullable(requestAst.getUnitCount()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setUnitCount(Long.parseLong(text));
        });
        Optional.ofNullable(requestAst.getUnitIndex()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setUnitIndex(Long.parseLong(text));
        });
        Optional.ofNullable(requestAst.getStep()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setStep( Long.parseLong(text));
        });
        Optional.ofNullable(requestAst.getStep()).map(i -> i.toString())
                .map(i -> SQLUtils.normalize(i)).ifPresent(text -> {
            config.setStep( Long.parseLong(text));
        });
        try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
            ops.putSequence(config);
            ops.commit();
        }
        response.sendOk();
    }


}
