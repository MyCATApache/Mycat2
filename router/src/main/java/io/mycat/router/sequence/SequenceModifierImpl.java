/**
 * Copyright (C) <2020>  <chen junwen>
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

package io.mycat.router.sequence;

import io.mycat.api.MySQLAPIRuntime;
import io.mycat.sequencemodifier.ModifyCallback;
import io.mycat.sequencemodifier.SequenceModifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SequenceModifierImpl implements SequenceModifier<MySQLAPIRuntime> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SequenceModifierImpl.class);
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