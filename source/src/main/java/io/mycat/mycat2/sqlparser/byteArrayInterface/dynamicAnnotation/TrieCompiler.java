package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.byteArrayInterface.ByteArrayInterface;
import io.mycat.mycat2.sqlparser.byteArrayInterface.Tokenizer2;

import java.util.*;
import java.util.stream.Collectors;

import static io.mycat.mycat2.sqlparser.byteArrayInterface.Tokenizer2.QUESTION_MARK;

public class TrieCompiler {

    boolean isTrie;
    HashMap<TrieKey, TrieCompiler> children = new HashMap<>();
    Set<String> callback;
    int backPos = 0;

    public static boolean insertNode(BufferSQLContext context, TrieCompiler head, String runnable, int backPos) {
        HashArray array = context.getHashArray();
        ByteArrayInterface byteArray = context.getBuffer();
        if (array == null || array.getCount() == 0)
            return false;
        int i = 0;
        TrieCompiler cur = head;
        //将字符串的每个字符插入到前缀树中
        int length = array.getCount();
        while (i < length) {
            TrieKey c = new TrieKey(array.getType(i), array.getHash(i), byteArray.getStringByHashArray(i, array));
            if (!cur.children.containsKey(c)) {
                TrieCompiler trieCompiler = new TrieCompiler();
                cur.children.put(c, trieCompiler);
                //如果当前节点中的子树节点中不包含当前字符，新建一个子节点。
            }
            //否则复用该节点
            cur = cur.children.get(c);
            if (cur.isTrie) {
                cur.backPos = backPos;
                doCallback(cur, runnable);
                System.out.println(" trie tree");
                return true;
                //判断前缀树中是否有字符串为当前字符串的前缀。
            }
            i++;
        }
        cur.isTrie = true;
        if (cur.children.size() > 0) {
            cur.backPos = backPos;
            doCallback(cur, runnable);
            System.out.println(" trie tree");
            return true;
            //判断当前字符串是否是前缀树中某个字符的前缀。
        }
        cur.backPos = backPos;
        doCallback(cur, runnable);
        return false;
    }

    public static void doCallback(TrieCompiler head, String runnable) {
        if (head.callback == null) {
            head.callback = new HashSet<>();
        }
        head.callback.add(runnable);
    }

    public static void main(String[] args) {
        TrieCompiler trieCompiler = new TrieCompiler();
    }

    @Override
    public String toString() {
        return "TrieCompiler{" +
                "isTrie=" + isTrie +
                ", children=" + children +
                ", callback=" + callback +
                '}';
    }

    public String toCode1(String className, TrieContext context) {
        String body = toCode2(true, context);
        String tmpl = String.format("\n" +
                "package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;\n" +
                "\n" +
                "import io.mycat.mycat2.sqlparser.BufferSQLContext;\n" +
                "import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;\n" +
                "import io.mycat.mycat2.sqlparser.byteArrayInterface.ByteArrayInterface;" +
                "public class %s implements DynamicAnnotationMatch {\n" +
                "    public final void pick(int i, final int arrayCount, BufferSQLContext context, HashArray array, ByteArrayInterface sql) {\n" +
                "int res;" +
                "        while (i < arrayCount) {\n" +
                "            res = pick0(i, arrayCount, context, array, sql);\n" +
                "            if (res == i) {\n" +
                "                ++i;\n" +
                "            } else {\n" +
                "                i = res;\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "\n" +
                "    public final int pick0(int i, final int arrayCount, BufferSQLContext context, HashArray array, ByteArrayInterface sql) {\n" +
                "  %s" +
                "        return i;\n" +
                "    }%s}", className, body, context.funList.stream().collect(Collectors.joining(" ")));
        return tmpl;
    }

    public String toCode2(boolean isRoot, TrieContext context) {
        context.x += 1;
        Map<Boolean, List<Map.Entry<TrieKey, TrieCompiler>>> map = this.children.entrySet().stream().collect(Collectors.partitioningBy((k) -> k.getKey().getType() == QUESTION_MARK));
        String l = toCode3(isRoot, map.get(Boolean.TRUE), context);
        String r = toCode3(isRoot, map.get(Boolean.FALSE), context);
        context.x -= 1;
        return l + r;
    }

    public String toCode3(boolean isRoot, List<Map.Entry<TrieKey, TrieCompiler>> entrySet, TrieContext context) {

        if (context.x > 3  || context.y > 5  ) {
            String res = toCode4(isRoot, entrySet, context);
            if (!"".equals(res.trim())) {
                String funName = context.genFun(entrySet.stream().map((s) -> AscllUtil.shiftAscll(s.getKey().getText(), false)).collect(Collectors.joining("_"))) + "_" + context.index;
                context.funList.add("\npublic  final int " + funName + "(int i, final int arrayCount, BufferSQLContext context, HashArray array, ByteArrayInterface sql){\n" + res + "\nreturn i;}");
                if(context.isBacktracking&& funName.contains("QUESTIONMARK")){
                    return "\n" + funName + "(i" +
                            "\n, arrayCount, context, array, sql);\n";
                }else{
                    return "\ni=" + funName + "(i, arrayCount, context, array, sql);\n";
                }

            }
            return "";
        } else {
            return toCode4(isRoot, entrySet, context);
        }
    }

    public String toCode4(boolean isRoot, List<Map.Entry<TrieKey, TrieCompiler>> entrySet, TrieContext context) {
        StringBuilder stringBuilder = new StringBuilder();
        context.y += 1;
        for (Map.Entry<TrieKey, TrieCompiler> i : entrySet) {
            context.index += 1;
            String q ="\n"+ "if ((i)<arrayCount&&i>-1){"+"\n";
            int type = i.getKey().getType();
            switch (type) {
                case Tokenizer2.DIGITS: {
                    q += "\nif(context.matchDigit(i," + i.getKey().getText() + ")){\n";
                    q += "\n++i;";
                    break;
                }
                case QUESTION_MARK: {
                    q += "\n{int start=i;i=context.matchPlaceholders(i);\n";
                    break;
                }
                default: {
                    if (i.getKey().longHash == 0) {
                        q += "\nif(" + i.getKey().getType() + "L==array.getType(i)){\n";
                    } else {
                        q += "\nif(" + i.getKey().getLongHash() + "L==array.getHash(i)){\n";
                    }
                    q += "\n++i;";
                    break;
                }
            }

            Set<String> callback = i.getValue().callback;
            String w = "";
            if (callback != null && callback.size() != 0) {
                Iterator<String> iterator = callback.iterator();
                while (iterator.hasNext()) {
                    w += "\ncontext.setDynamicAnnotationResult(" + iterator.next() + ");";
                }
                if (context.isBacktracking&&type==QUESTION_MARK){
                    w+="\npick(start-" +
                            +i.getValue().backPos+", arrayCount, context, array, sql);\n";
                } else

                if (i.getValue().backPos != 0) {
                    w += "\ni=pick0(i-" + i.getValue().backPos +
                            ", arrayCount, context, array, sql);\n";
                }

            }
            String e = i.getValue().toCode2(false, context);
            String r = "\n}\n}";
            stringBuilder.append(q).append(w).append(e).append(r);
            if (type == QUESTION_MARK) {
                String funName = context.genFun(entrySet.stream().map((s) -> AscllUtil.shiftAscll(s.getKey().getText(), false)).collect(Collectors.joining("_"))) + "_" + context.index;
                funName += "_quest";
                context.funList.add("\npublic  final int " + funName + "(int i, final int arrayCount, BufferSQLContext context, HashArray array, ByteArrayInterface sql){\n" + stringBuilder.toString() + "\nreturn i;}\n");
                stringBuilder.setLength(0);
                stringBuilder.append((context.isBacktracking?"":"i=" )+ funName + "(i, arrayCount, context, array, sql);\n");
            }
        }
        context.y -= 1;
        return stringBuilder.toString();
    }
}