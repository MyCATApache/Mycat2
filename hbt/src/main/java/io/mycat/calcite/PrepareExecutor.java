package io.mycat.calcite;

import io.reactivex.rxjava3.core.Observable;
import lombok.Getter;

@Getter
public class PrepareExecutor {
   final PrepareExecutorType type;
   final Observable executor;

    public PrepareExecutor(PrepareExecutorType type, Observable executor) {
        this.type = type;
        this.executor = executor;
    }

    public static  PrepareExecutor of(PrepareExecutorType type, Observable executor){
        return new PrepareExecutor(type,executor);
    }

}
