/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.configRaft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zongtanghu
 */
public class MycatPriorityElectionOnlyStateMachine<T> extends StateMachineAdapter {

    private static final Logger LOG = LoggerFactory
            .getLogger(MycatPriorityElectionOnlyStateMachine.class);

    private final AtomicLong leaderTerm = new AtomicLong(-1L);
    private final List<MycatLeaderStateListener> listeners;
     final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

    public MycatPriorityElectionOnlyStateMachine(List<MycatLeaderStateListener> listeners) {
        this.listeners = listeners;
    }

    @Override
    public void onApply(final Iterator it) {
        ObjectMapper objectMapper = new ObjectMapper();
        // election only, do nothing
        while (it.hasNext()) {
            LOG.info("On apply with term: {} and index: {}. ", it.getTerm(), it.getIndex());
            final MycatTaskClosure closure =(MycatTaskClosure) it.done();
            ByteBuffer data = it.getData();
            String json;
            {
                if (data.remaining()==0){
                    it.next();
                    continue;//heart beat
                }
                final byte[] cmdBytes = new byte[data.remaining()];
                data.get(cmdBytes);
                json = new String(cmdBytes);
            }
            MycatEntry entry;
            try {
                entry = objectMapper.readValue(json, MycatEntry.class);
            } catch (JsonProcessingException e) {
                LOG.error(json, e);
                LOG.error(json, e);
                throw new RuntimeException(e);
            }
            String cmd = entry.getCmd();
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            Object response = null;
            if ("GET".equalsIgnoreCase(cmd)) {
                response = this.map.get(key);
            }
            if ("SET".equalsIgnoreCase(cmd)) {
                this.map.put(key, value);
                response = null;
                try {
                    for (MycatLeaderStateListener listener : listeners) {
                        listener.onSet(key, value,new HashMap<>(this.map));
                    }
                } catch (Throwable throwable) {
                    LOG.error("", throwable);
                }
            }
            if (closure != null) {
                closure.setResponse(response);
                closure.run(Status.OK());
            }
            it.next();
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        final Map<String, String> values = new HashMap<>(this.map);
        Utils.runInThread(() -> {
            final MycatConfigSnapshotFile snapshot = new MycatConfigSnapshotFile(writer.getPath() + File.separator + "data");
            if (snapshot.save(values)) {
                if (writer.addFile("data")) {
                    done.run(Status.OK());
                } else {
                    done.run(new Status(RaftError.EIO, "Fail to add file to writer"));
                }
            } else {
                done.run(new Status(RaftError.EIO, "Fail to save counter snapshot %s", snapshot.getPath()));
            }
        });
    }

    @Override
    public void onLeaderStart(final long term) {
        super.onLeaderStart(term);
        this.leaderTerm.set(term);
        for (final MycatLeaderStateListener listener : this.listeners) { // iterator the snapshot
            listener.onLeaderStart(term);
        }
    }

    @Override
    public void onLeaderStop(final Status status) {
        super.onLeaderStop(status);
        final long oldTerm = leaderTerm.get();
        this.leaderTerm.set(-1L);
        for (final MycatLeaderStateListener listener : this.listeners) { // iterator the snapshot
            listener.onLeaderStop(oldTerm);
        }
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    public void addLeaderStateListener(final MycatLeaderStateListener listener) {
        this.listeners.add(listener);
    }


    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot");
            return false;
        }
        if (reader.getFileMeta("data") == null) {
            LOG.error("Fail to find data file in {}", reader.getPath());
            return false;
        }
        final MycatConfigSnapshotFile snapshot = new MycatConfigSnapshotFile(reader.getPath() + File.separator + "data");
        try {
            final Map<String, String> values = snapshot.load();
            this.map.clear();
            if (values != null) {
                this.map.putAll(values);
            }
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to load snapshot from {}", snapshot.getPath());
            return false;
        }
    }
}
