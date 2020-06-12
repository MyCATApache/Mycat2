package io.mycat.metadata;

import io.mycat.BackendTableInfo;
import io.mycat.DataNode;
import io.mycat.router.ShardingTableHandler;

import java.util.List;

public interface Unperfected  extends ShardingTableHandler,GlobalTableHandler{

        public boolean isNatureTable() ;

        public List<DataNode> getShardingBackends();


    }