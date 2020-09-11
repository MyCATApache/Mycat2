package io.mycat;

import java.util.function.Supplier;

public class MetaClusterFactory implements Supplier<MetaCluster> {
    @Override
    public MetaCluster get() {
        return null;
    }
 public  static    MetaCluster    getInstance(){
     MetaCluster metaCluster = new MetaCluster(System.currentTimeMillis());
//     metaCluster.add(Repli)
     return null;
 }
}