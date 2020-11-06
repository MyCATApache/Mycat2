package io.mycat.beans.mysql;

import lombok.Getter;

import java.util.function.Consumer;


public enum  InformationSchemaRuntime {
   INSTANCE;
    final  InformationSchema SINGLE = new InformationSchema();
    public InformationSchema get(){
        return SINGLE;
    }
    public synchronized void update(Consumer<InformationSchema> consumer){
        consumer.accept(SINGLE);
    }

}