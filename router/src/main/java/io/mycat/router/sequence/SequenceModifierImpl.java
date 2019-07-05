package io.mycat.router.sequence;

import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.sequenceModifier.ModifyCallback;
import io.mycat.sequenceModifier.SequenceModifier;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SequenceModifierImpl implements SequenceModifier {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(SequenceModifierImpl.class);
  private final Pattern pattern;
  private final SequenceHandler sequenceHandler;

  public SequenceModifierImpl(String pattern, SequenceHandler sequenceHandler) {
    this.pattern = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
    this.sequenceHandler = sequenceHandler;
  }

  public void modify(String sql, ModifyCallback callback) {
    Matcher matcher = this.pattern.matcher(sql);
    if (!matcher.find()) {
      callback.onSuccessCallback(sql);
      return;
    } else {
      String name = matcher.group(2);
      sequenceHandler.nextId(new SequenceCallback() {
        String resSQL = sql;

        @Override
        public void onSequence(long value) {
          try {
            resSQL = resSQL.replaceFirst(matcher.group(1), Long.toString(value));
            if (matcher.find()) {
              sequenceHandler.nextId(this);
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
  public void init(Map<String, String> properties) {

  }

}