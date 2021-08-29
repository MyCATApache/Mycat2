/**
 * Copyright [2021] [chen junwen]
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.mycat.vertx.xa;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.Json;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ImmutableCoordinatorLog implements Serializable {
    private final static Logger LOGGER = LoggerFactory.getLogger(ImmutableCoordinatorLog.class);
    private final String xid;
    private final ImmutableParticipantLog[] participants;
    private final boolean commitMarked;//flag,means all participants are prepared.

    public ImmutableCoordinatorLog(String coordinatorId, ImmutableParticipantLog[] participants, boolean commit) {
        this.xid = coordinatorId;
        this.participants = participants;
        this.commitMarked = commit;
    }

    public ImmutableCoordinatorLog(String coordinatorId, ImmutableParticipantLog[] participants) {
        this(coordinatorId, participants, false);
    }

    public String getXid() {
        return xid;
    }

    public List<ImmutableParticipantLog> getParticipants() {
        return Arrays.asList(participants);
    }

    /**
     *
     *
     * @return  the min state for subsequent xa command to commit or rollback
     */
    public State computeMinState() {
        State txState = State.XA_COMMITED;
        if (Arrays.stream(participants).allMatch(i -> i.getState() == State.XA_COMMITED)) {
            return State.XA_COMMITED;
        }

        if (Arrays.stream(participants).allMatch(i -> i.getState() == State.XA_ROLLBACKED)) {
            return State.XA_ROLLBACKED;
        }
        for (ImmutableParticipantLog participant : participants) {
            if (txState == State.XA_INITED) {
                return State.XA_INITED;
            }
            if (txState.compareTo(participant.getState()) > 0) {
                txState = participant.getState();
            }
        }
        return txState;
    }

    /**
     *
     * local commit flag log means all participants has prepared but not commited
     * if Coordinator keep not occurs broken and received the response,it is Strong consistency.
     *
     * @param state check state
     * @return the received xa state + local commit flag log
     */
    public boolean mayContains(State state) {
        if (commitMarked) {
            return State.XA_COMMITED == state;
        }
        return Arrays.stream(participants).anyMatch(immutableParticipantLog -> state == immutableParticipantLog.getState());
    }

    public ImmutableParticipantLog[] replace(String target, State state, long expires) {
        ArrayList<ImmutableParticipantLog> res = new ArrayList<>();
        boolean find = false;
        for (ImmutableParticipantLog participant : participants) {
            if (participant.getTarget().equals(target)) {
                find = true;
                res.add(participant.copy(state));
            } else {
                res.add(participant);
            }
        }
        if (!find) {
            res.add(new ImmutableParticipantLog(target, expires, state));
        }
        return res.toArray(new ImmutableParticipantLog[]{});
    }

    public long computeExpires() {
        return Arrays.stream(participants).mapToLong(i -> i.getExpires()).max().orElse(0);
    }

    public ImmutableCoordinatorLog withCommit(boolean success) {
        return new ImmutableCoordinatorLog(xid, getParticipants().toArray(new ImmutableParticipantLog[0]), success);
    }

    public String toJson() {
        return Json.encodePrettily(new MutableCoordinatorLog(getXid(), getParticipants().toArray(new ImmutableParticipantLog[0]), commitMarked));
    }

    public static ImmutableCoordinatorLog from(String text) {
        MutableCoordinatorLog mutableCoordinatorLog = Json.decodeValue(text, MutableCoordinatorLog.class);
        return new ImmutableCoordinatorLog(mutableCoordinatorLog.getXid(),
                mutableCoordinatorLog.getParticipants(),
                mutableCoordinatorLog.isCommitMarked());
    }

    public static class MutableCoordinatorLog {

        private String xid;
        private ImmutableParticipantLog[] participants;
        private boolean commitMarked;

        public MutableCoordinatorLog(String xid, ImmutableParticipantLog[] participants, boolean commitMarked) {
            this.xid = xid;
            this.participants = participants;
            this.commitMarked = commitMarked;
        }

        public String getXid() {
            return xid;
        }

        public void setXid(String xid) {
            this.xid = xid;
        }

        public ImmutableParticipantLog[] getParticipants() {
            return participants;
        }

        public void setParticipants(ImmutableParticipantLog[] participants) {
            this.participants = participants;
        }

        public boolean isCommitMarked() {
            return commitMarked;
        }

        public void setCommitMarked(boolean commitMarked) {
            this.commitMarked = commitMarked;
        }

    }
}
