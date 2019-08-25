package cn.lightfish.sql.ast.function;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.function.Function;

public enum FunctionManager {
  INSTANCE;
 final private HashMap<String, DefFunction> map = new HashMap<>();

  FunctionManager() {
    register(CastFunction.class);
    register(StringFunction.class);
    register(DateFunction.class);
  }

  private void register(Class functions) {
    if (functions == null) {
      return;
    }
    Method[] methods = functions.getMethods();
    for (Method method : methods) {
      if (!Modifier.isStatic(method.getModifiers())) {
        continue;
      }
      if (method.getDeclaringClass() == Object.class) {
        continue;
      }
      String name = method.getName();
      Type returnType = method.getGenericReturnType();
      Type[] paramTypes = method.getGenericParameterTypes();

      boolean isVarArgs = method.isVarArgs();
      if (isVarArgs) {
        Type lastType = paramTypes[paramTypes.length - 1];
        Type componentType = ((Class) lastType).getComponentType();
        paramTypes[paramTypes.length - 1] = componentType;
      } else {
        try {
          registerFunction(name, isVarArgs, returnType, paramTypes,
              MethodHandles.lookup().unreflect(method));
        } catch (Exception e) {
          System.out.println(e);
        }
      }
    }
  }

  private void registerFunction(String name, boolean isVarArgs, Type returnType,
      Type[] paramTypes, MethodHandle methodHandle) {
    map.put(name,
        new DefFunction(name, isVarArgs, (Class[]) paramTypes, (Class) returnType, methodHandle));
    System.out.println();
  }

  public static void main(String[] args) {
    Function binary = FunctionManager.INSTANCE.getFunctionByName("BINARY");
    Object apply = binary.apply("1");
  }

  public Function getFunctionByName(String functionName) {
    Function function = map.get(functionName);
    if (function == null){
      throw new UnsupportedOperationException();
    }
    return function;
  }
}