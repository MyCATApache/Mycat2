package cn.lightfish.describer;

import cn.lightfish.describer.literal.*;
import cn.lightfish.wu.BaseQuery;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

public class EvalNodeVisitor2 implements ParseNodeVisitor {
    private final List<Class> clazzList;

    Map<FunctionSig, Builder> map = new HashMap<>();
    LinkedList<Object> stack = new LinkedList<>();
    Map<String, List<FunctionSig>> mapping = new HashMap<>();

    public EvalNodeVisitor2(Class clazz) throws IllegalAccessException {
        this(Collections.singletonList(clazz));
    }

    public EvalNodeVisitor2(List<Class> clazzList) throws IllegalAccessException {
        this.clazzList = clazzList;
        for (Class clazz : clazzList) {
            Method[] methods = clazz.getMethods();
            for (Method method : methods) {
                FunctionSig functionSig = functionSig(method);
                List<FunctionSig> sigs;
                if (!mapping.containsKey(functionSig.getName())) {
                    mapping.put(functionSig.getName(), sigs = new ArrayList<>());
                } else {
                    sigs = mapping.get(functionSig.getName());
                }
                sigs.add(functionSig);
            }
        }
    }

    public static void main(String[] args) throws IllegalAccessException {
        EvalNodeVisitor2 evalNodeVisitor2 = new EvalNodeVisitor2(BaseQuery.class);
        String text = "distinct(valuesSchema(fields(fieldType(`id`,`int`)),values()))";
        Describer describer = new Describer(text);
        ParseNode expression = describer.expression();
        expression.accept(evalNodeVisitor2);
    }

    private FunctionSig functionSig(Method method) throws IllegalAccessException {
        return new FunctionSig(method.getName(), method);
    }

    private <T> T cast(ParseNode node) {
        return (T) (node);
    }

    @Override
    public void visit(Bind bind) {
        bind.expr.accept(this);
    }

    @Override
    public void endVisit(Bind bind) {

    }

    @Override
    public void visit(CallExpr call) {
        accept(call.getArgs());
    }

    @Override
    public void endVisit(CallExpr call) {
        FunctionSig methodHandle;
        methodHandle = getHandle(call);
        if (methodHandle == null) {
            System.out.println();
        }
        int size = call.getArgs().getExprs().size();
        List<Object> callArgs = methodHandle.getCallArgs(stack.subList(0, size));
        try {
            Method handle = methodHandle.getHandle();
            Object o = handle.invoke(null, callArgs.toArray());
            for (int i = 0; i < size; i++) {
                stack.pop();
            }
            stack.push(o);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

    }

    @Nullable
    private FunctionSig getHandle(CallExpr call) {
        List<FunctionSig> sigs = mapping.get(call.getName());
        List<Class> classList = getType();
        System.out.println(sigs);
        System.out.println((call.getName() + ":" + classList));
        int argLength = classList.size();

        for (FunctionSig sig : sigs) {
            int min = Math.min(argLength, sig.getParaLength());
            int i = 0;
            for (; i < min; i++) {
                Class argType = classList.get(i);
                Class paraType = sig.getParaType(i);
                if (paraType == null) {
                    break;
                }
                if (paraType.isAssignableFrom(argType)) {
                    continue;
                } else {
                    break;
                }
            }
            if (i == min) {
                return sig;
            } else {
                continue;
            }
        }
        return null;
    }

    List<Class> getType() {
        return stack.stream().map(i -> i.getClass()).collect(Collectors.toList());
    }

    @Override
    public void visit(IdLiteral id) {

    }

    @Override
    public void endVisit(IdLiteral id) {
        stack.push(BaseQuery.id(id.getId()));
    }

    @Override
    public void visit(ParenthesesExpr parenthesesExpr) {
        accept(parenthesesExpr.getExprs());


    }

    private void accept(ParseNode expr) {
        expr.accept(this);
    }

    private void accept(List<ParseNode> expr) {
        for (ParseNode parseNode : expr) {
            parseNode.accept(this);
        }

    }

    @Override
    public void endVisit(ParenthesesExpr parenthesesExpr) {

    }

    @Override
    public void visit(IntegerLiteral numberLiteral) {

    }

    @Override
    public void endVisit(IntegerLiteral numberLiteral) {
        stack.push(BaseQuery.literal(numberLiteral.getNumber()));
    }

    @Override
    public void visit(StringLiteral stringLiteral) {

    }

    @Override
    public void endVisit(StringLiteral stringLiteral) {
        stack.push(BaseQuery.literal(stringLiteral.getString()));
    }

    @Override
    public void visit(DecimalLiteral decimalLiteral) {

    }

    @Override
    public void endVisit(DecimalLiteral decimalLiteral) {
        stack.push(BaseQuery.literal(decimalLiteral.getNumber()));
    }

    @Override
    public void visit(PropertyLiteral propertyLiteral) {

    }

    @Override
    public void endVisit(PropertyLiteral propertyLiteral) {

    }

    @EqualsAndHashCode

    @Getter
    @ToString
    static class FunctionSig {
        private final List<Class<?>> classes;
        private final int length;
        String name;
        Method handle;
        int paraLength;

        public FunctionSig(String name, Method handle) {
            this.name = name;
            this.handle = handle;
            this.classes = Arrays.asList(handle.getParameterTypes());
            this.length = classes.size();
            this.paraLength = getParaLength();

        }

        @SneakyThrows
        public List<Object> getCallArgs(List<Object> objects) {

            if (length == 0) {
                return Collections.emptyList();
            }
            if (isArray() && objects.isEmpty()) {
                return Collections.singletonList(Array.newInstance(getArrayType(), 0));
            }
            if (length == 1 && !isArray()) {
                return Collections.singletonList(objects.get(0));
            }
            if (length > 1 && !isArray()) {
                return new ArrayList<>(objects);
            }
            if (length == 1 && isArray()) {
                Object o1 = objects.get(0);
                if (o1.getClass().isArray()) {
                    return Collections.singletonList(o1);
                } else {
                    Object o = Array.newInstance(getArrayType(), 1);
                    Array.set(o, 0, o1);
                    return Collections.singletonList(o);
                }
            }

            if (length > 1 && isArray()) {
                Object o = Array.newInstance(getArrayType(), objects.size() - length - 1);
                int index = 0;
                for (Object o1 : objects.subList(length - 1, objects.size())) {
                    Array.set(o, index++, o1);
                }
                ArrayList<Object> objects1 = new ArrayList<>(length);
                objects1.addAll(objects.subList(0, length - 1));
                objects1.add(o);
                return objects1;
            }
            throw new UnsupportedOperationException();
        }

        private Class<?> getArrayType() {
            return classes.get(classes.size() - 1).getComponentType();
        }

        private boolean isArray() {
            return paraLength == Integer.MAX_VALUE;
        }

        private int getParaLength() {
            if (classes.isEmpty()) {
                return 0;
            } else {
                Class<?> aClass = getLastClass();
                if (aClass.isArray()) {
                    return Integer.MAX_VALUE;
                } else {
                    return classes.size();
                }
            }
        }

        private Class<?> getLastClass() {
            return classes.get(classes.size() - 1);
        }

        public Class getParaType(int i) {
            if (i < length - 1) {
                return classes.get(i);
            } else {
                if (isArray()) {
                    return getArrayType();
                } else {
                    return classes.get(i);
                }
            }
        }
    }
}