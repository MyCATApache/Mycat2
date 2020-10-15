package io.mycat.config;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
    @Builder
    @EqualsAndHashCode
    public  class MycatCommandConfig {
        String name;
        String clazz;

        public MycatCommandConfig(String name, String clazz) {
            this.name = name;
            this.clazz = clazz;
        }

        public MycatCommandConfig() {
        }
    }