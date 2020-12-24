package io.mycat;

import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class MycatClassResolver {
    @SneakyThrows
    public static void forceStaticSet(Class target, String filedName, Object value) {
        Field mapField = target.getDeclaredField(filedName);
        if (!mapField.isAccessible()) {
            mapField.setAccessible(true);
        }
        mapField.set(value, mapField.getModifiers() & ~Modifier.FINAL);
    }

    @SneakyThrows
    public static <T> T forceStaticGet(@NotNull Class targetClass, Object target, String filedName) {
        Field mapField = targetClass.getDeclaredField(filedName);
        if (!mapField.isAccessible()) {
            mapField.setAccessible(true);
        }
        return (T) mapField.get(target);
    }
}