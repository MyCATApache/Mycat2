package io.mycat.router.sequence;

import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.mysqlapi.MySQLAPIRuntime;
import io.mycat.sequenceModifier.ModifyCallback;
import io.mycat.sequenceModifier.SequenceModifier;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SequenceModifierImpl implements SequenceModifier {
  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(SequenceModifierImpl.class);
  private Pattern pattern;
  private SequenceHandler sequenceHandler;

  public void modify(String schema, String sql, ModifyCallback callback) {
    Matcher matcher = this.pattern.matcher(sql);
    if (!matcher.find()) {
      callback.onSuccessCallback(sql);
      return;
    } else {
      String seqName = matcher.group(2);
      sequenceHandler.nextId(schema, seqName, new SequenceCallback() {
        String resSQL = sql;

        @Override
        public void onSequence(long value) {
          try {
            resSQL = resSQL.replaceFirst(matcher.group(1), " " + value + " ");
            if (matcher.find()) {
              sequenceHandler.nextId(schema, seqName, this);
            } else {
              callback.onSuccessCallback(resSQL);
            }
          } catch (Exception e) {
            callback.onException(e);
          }
        }

        @Override
        public void onException(Exception e) {
          callback.onException(e);
        }
      });
    }
  }

  @Override
  public void init(MySQLAPIRuntime mySQLAPIRuntime,
      Map<String, String> properties) {
    String sequenceHandler = properties.get("sequenceHandlerClass");
    this.pattern = Pattern.compile(properties.get("pattern"), Pattern.CASE_INSENSITIVE);
    try {
      Class<?> aClass = Class.forName(sequenceHandler);
      SequenceHandler o = (SequenceHandler) aClass.newInstance();
      o.init(mySQLAPIRuntime, properties);
      this.sequenceHandler = o;
    } catch (Exception e) {
      LOGGER.error("{}", e);
    }
  }

}