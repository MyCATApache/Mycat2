package io.mycat.metadata;

import io.mycat.BackendTableInfo;

import java.util.List;

public interface Unperfected  extends ShardingTableHandler,GlobalTableHandler{

        public boolean isNatureTable() ;

        public List<BackendTableInfo> getShardingBackends();


    }