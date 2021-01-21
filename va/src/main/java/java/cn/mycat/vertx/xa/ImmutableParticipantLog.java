/**
 * Copyright [2021] [chen junwen]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package java.cn.mycat.vertx.xa;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

public class ImmutableParticipantLog {
    private final static Logger LOGGER = LoggerFactory.getLogger(ImmutableParticipantLog.class);
    private final String target;
    private final long expires;
    private final State state;


    public ImmutableParticipantLog(String target,
                                   long expires,
                                   State txState) {
        this.target = target;
        this.expires = expires;
        this.state = txState;
    }

    public String getTarget() {
        return target;
    }

    public long getExpires() {
        return expires;
    }

    public State getState() {
        return state;
    }

    public ImmutableParticipantLog copy(State state){
        return new ImmutableParticipantLog(getTarget(),getExpires(),state);
    }

}
