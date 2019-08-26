package cn.lightfish.sql.ast.expr.functionExpr;

import java.lang.invoke.MethodHandle;
import java.util.function.Function;


public class DefFunction implements Function<Object, Object> {

  final String methodName;
  final boolean varArgs;
  final Class[] parameterTypes;
  final Class returnType;
  MethodHandle handle;

  public DefFunction(String methodName, boolean varArgs, Class[] parameterTypes,
      Class returnType, MethodHandle handle) {
    this.methodName = methodName;
    this.varArgs = varArgs;
    this.parameterTypes = parameterTypes;
    this.returnType = returnType;
    this.handle = handle;
  }

  private Object invoke() {
    try {
      return handle.invoke();
    } catch (Throwable throwable) {
      throwable.printStackTrace();
      return null;
    }
  }

  private Object invoke(Object... args) {
    try {
      if (args == null || args.length == 0 || args[0] == null) {
        return handle.invoke();
      } else {
        return handle.invokeWithArguments(args);
      }
    } catch (Throwable throwable) {
      throwable.printStackTrace();
      return null;
    }
  }

  @Override
  public Object apply(Object objects) {
    return objects==null? invoke(): invoke(objects);
  }
}