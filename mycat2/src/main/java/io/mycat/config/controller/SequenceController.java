package io.mycat.config.controller;

import com.google.common.collect.ImmutableList;
import io.mycat.MetaClusterCurrent;
import io.mycat.config.SequenceConfig;
import io.mycat.config.ServerConfig;
import io.mycat.plug.sequence.SequenceGenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SequenceController {

    public static void update(List<SequenceConfig> sequenceConfigs) {
        ServerConfig serverConfig = MetaClusterCurrent.wrapper(ServerConfig.class);
        SequenceGenerator sequenceGenerator = new SequenceGenerator(serverConfig.getMycatId(), sequenceConfigs);
        MetaClusterCurrent.register(SequenceGenerator.class, sequenceGenerator);
    }

    public static void add(SequenceConfig sequenceConfig) {
        List<SequenceConfig> list = (List) ImmutableList.builder().addAll(MetaClusterCurrent.wrapper(SequenceGenerator.class).getSequencesConfig()).add(sequenceConfig).build();
        update(list);
    }

    public static void remove(String sequenceConfig) {
        List<SequenceConfig> list = new ArrayList<>(MetaClusterCurrent.wrapper(SequenceGenerator.class).getSequencesConfig());
        list.removeIf(s->s.getName().equalsIgnoreCase(sequenceConfig));
        update(list);
    }
}
