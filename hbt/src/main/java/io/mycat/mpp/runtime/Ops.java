package io.mycat.mpp.runtime;

import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

public class Ops {

    public static void registed(Map map, Class aClass) {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        for (Method method : aClass.getMethods()) {
            if (Modifier.isStatic(method.getModifiers())) {
                Class<?>[] parameterTypes = method.getParameterTypes();
                MethodHandle unreflect = null;
                try {
                    unreflect = lookup.unreflect(method);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
                Ops.Key key = Ops.Key.of(parameterTypes, method.getReturnType());
                map.put(key, unreflect);
            }
        }
    }

    @SneakyThrows
    public static Invoker resolve(Map map, Type... types) {
        Class[] classes = new Class[types.length];
        int index = 0;
        for (Type type : types) {
            classes[index++] = type.getJavaClass();
        }
        MethodHandle methodHandle = Objects.requireNonNull((MethodHandle) map.get(Key.of(classes)));
        return arguments -> methodHandle.invokeWithArguments(arguments);
    }

    public static Invoker resolveReturnBoolean(Map map, Type... types) {
        Class[] classes = new Class[types.length+1];
        int index = 0;
        for (Type type : types) {
            classes[index++] = type.getJavaClass();
        }
        classes[index] = Boolean.TYPE;
        MethodHandle methodHandle = (MethodHandle) map.get(Key.of(classes));
        return arguments -> methodHandle.invokeWithArguments(arguments);
    }

    @EqualsAndHashCode
    public static final class Key {
        final Class[] argType;

        public static Key of(List<Class> argTypes) {
            return new Key(argTypes.toArray(new Class[0]));
        }

        public static Key of(Class[] argTypes) {
            return new Key(argTypes);
        }

        public static Key of(Class[] argTypes, Class returnType) {
            ArrayList<Class> objects = new ArrayList<>(Arrays.asList(argTypes));
            objects.add(returnType);
            return new Key(objects.toArray(new Class[0]));
        }

        public Key(List<Class> argType) {
            this.argType = argType.toArray(new Class[0]);
        }

        public Key(Class[] argType) {
            this.argType = argType;
        }
    }
}