package io.mycat.sqlhandler.config;

import io.mycat.config.KVObject;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@AllArgsConstructor
public class UpdateSet<T extends KVObject> {
    Collection<T> create;
    Collection<T> delete;
    Collection<T> target;

    public static <T extends KVObject> UpdateSet<T> of(Collection<T> create,
                                                       Collection<T> delete, Collection<T> target) {
        return new UpdateSet<T>(create, delete, target);
    }

    public static <T extends KVObject> UpdateSet<T> create(Collection<T> n,
                                                           Collection<T> o) {
        return of(computeNeedCreate(n, o), computeNeedDelete(n, o), new ArrayList<>(n));
    }

    public boolean isEmpty() {
        return create.isEmpty() && delete.isEmpty();
    }

    public void execute(KV<T> kv) {
        for (T t : this.getDelete()) {
            kv.removeKey(t.keyName());
        }
        for (T t : this.getCreate()) {
            String schemaName = t.keyName();
            kv.put(schemaName, t);
        }
    }

    public static <T> Collection<T> computeNeedCreate(Collection<T> newCollection, Collection<T> oldCollection) {
        ArrayList<T> n = new ArrayList<>(newCollection);
        ArrayList<T> o = new ArrayList<>(oldCollection);
        n.removeAll(o);
        return n;
    }

    public  static <T> Collection<T> computeNeedDelete(Collection<T> newCollection, Collection<T> oldCollection) {
        ArrayList<T> n = new ArrayList<>(newCollection);
        ArrayList<T> o = new ArrayList<>(oldCollection);
        o.removeAll(n);
        return o;
    }

    public Collection<T> getCreate() {
        return create.stream().distinct().collect(Collectors.toList());
    }

    public Collection<T> getDelete() {
        return delete.stream().distinct().collect(Collectors.toList());
    }

    public Collection<T> getTarget() {
        return target.stream().distinct().collect(Collectors.toList());
    }

    public  Map<String, T> getTargetAsMap() {
        return (Map<String, T>) target.stream().distinct().collect(Collectors.toMap(k -> k.keyName(), v -> v));
    }
    public List< T> getTargetAsList() {
        return new ArrayList( target);
    }
}
