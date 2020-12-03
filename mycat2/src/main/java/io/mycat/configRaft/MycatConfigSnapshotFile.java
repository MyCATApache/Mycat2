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


import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * atomic snapshot file
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 5:14:55 PM
 */
public class MycatConfigSnapshotFile {
    private static final Logger LOG = LoggerFactory.getLogger(MycatConfigSnapshotFile.class);
    private String              path;

    public MycatConfigSnapshotFile(String path) {
        super();
        this.path = path;
    }

    public String getPath() {
        return this.path;
    }

    /**
     * Save value to snapshot file.
     * @param values
     * @return
     */
    public boolean save(Map<String, String> values) {
        try {
            FileUtils.writeByteArrayToFile(new File(path), MycatCommandCodec.encodeCommand(values));
            return true;
        } catch (IOException e) {
            LOG.error("Fail to save snapshot", e);
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> load() throws IOException {
        byte[] bs = FileUtils.readFileToByteArray(new File(path));
        if (bs != null && bs.length > 0) {
            return MycatCommandCodec.decodeCommand(bs, Map.class);
        }
        throw new IOException("Fail to load snapshot from " + path + ",content: " + Arrays.toString(bs));
    }
}
