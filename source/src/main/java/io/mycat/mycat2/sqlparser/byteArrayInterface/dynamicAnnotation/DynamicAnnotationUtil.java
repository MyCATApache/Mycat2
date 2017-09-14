package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.BufferSQLParser;

import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by jamie on 2017/9/13.
 */
public class DynamicAnnotationUtil {
    static final DynamicClassLoader classLoader;
    static int name = 1;

    static {
        classLoader = new DynamicClassLoader("", Thread.currentThread().getContextClassLoader());
    }

    public static DynamicAnnotationRuntime compile(Map<Boolean, List< String>> lines) throws Exception {
        String filename = "_" + name++;
        DynamicAnnotationRuntime runtime = genJavacode(filename, filename + ".java", lines);
        compileJavaCodeToClass(runtime);
        loadClass(runtime);
        return runtime;
    }
    public static DynamicAnnotationRuntime compile( List< String> lines) throws Exception {
        HashMap<Boolean,List< String> > map=new HashMap<>();
        map.put(Boolean.TRUE,lines);
        return compile(map);
    }

    public static DynamicAnnotationRuntime genJavacode(String className, String path, Map<Boolean, List< String>> lines) throws IOException {
        DynamicAnnotationRuntime runtime = new DynamicAnnotationRuntime();
        runtime.setMatchName(className);
        String code = assemble(lines, runtime);
        Path p = Paths.get(path);
        System.out.println(p.toAbsolutePath());
        try (FileWriter fileWriter = new FileWriter(path)) {
            fileWriter.write(code);
        }
        runtime.setCodePath(path);
        return runtime;
    }

    public static void compileJavaCodeToClass(DynamicAnnotationRuntime runtime) throws Exception {
        compileTheJavaSrcFile(new File(runtime.codePath));
    }

    public static DynamicAnnotationMatch loadClass(DynamicAnnotationRuntime runtime) throws Exception {
        DynamicAnnotationMatch aClass = (DynamicAnnotationMatch) classLoader.findClass(runtime.codePath.replace(".java", ".class")).newInstance();
        runtime.setMatch(aClass);
        return aClass;
    }

    public static void compileTheJavaSrcFile(File... srcFiles) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        try (StandardJavaFileManager fileMgr = compiler.getStandardFileManager(null, null, null)) {
            JavaCompiler.CompilationTask t = compiler.getTask(null, fileMgr, null, null, null, fileMgr.getJavaFileObjects(srcFiles));
            t.call();
        } catch (Throwable e) {
            throw new RuntimeException("Fail to compile files [" + srcFiles + "]", e);
        }
    }

    private static String assemble(Map<Boolean, List<String>> map, DynamicAnnotationRuntime runtime) {
        List<String> list = map.values().stream().flatMap(maps -> maps.stream()).collect(Collectors.toList());
        preProcess(list, runtime);
        TrieCompiler trieCompiler = new TrieCompiler();
        TrieContext context = new TrieContext();
        Map<String, Integer> str2Int = runtime.str2Int;
        Map<String,Integer>backtrackingTable=runtime.backtrackingTable;
        for (Map.Entry<String, Integer> i : str2Int.entrySet()) {
            insert(i.getKey(), trieCompiler, i.getValue().toString(), backtrackingTable.get(i.getKey()));
        }
        return  trieCompiler.toCode1(runtime.matchName, runtime, context,map);
    }

    private static DynamicAnnotationRuntime preProcess(List<String> list, DynamicAnnotationRuntime runtime) {
        Map<String, Set<String>> relationTable = new HashMap<>();
        Map<String, Integer> backtrackingTable = new HashMap<>();
        BiFunction<String, String, Boolean> equal = (l, r) -> {
            if (l.equals(r)) return true;
            if (l.equals("?") || r.equals("?")) {
                return true;
            } else
                return false;
        };
        for (int i = 0; i < list.size(); i++) {
            for (int j = 0; j < list.size(); j++) {
                String one = list.get(i);
                String two = list.get(j);
                int pos = 0;//偏移量
                //查找共有后缀
                if (!one.equals(two) || list.size() == 1) {
                    String[] oneTokenList = one.split(" ");
                    String[] twoTokenList = two.split(" ");
                    int k = 0;
                    int l = 0;
                    int markPosOne = 0;
                    int markPosTwo = 0;
                    int max = 0;
                    for (k = 0; k < oneTokenList.length && k > -1 && l < twoTokenList.length; ) {
                        if (!equal.apply(oneTokenList[k], twoTokenList[l])) {
                            k++;
                            continue;
                        } else {
                            int m = k, n = l;
                            int tmpMax = 0;
                            for (; m < oneTokenList.length && n < twoTokenList.length; m++, n++) {
                                if (equal.apply(oneTokenList[m], (twoTokenList[n]))) {
                                    tmpMax++;
                                    continue;
                                } else {
                                    break;
                                }
                            }
                            if (max < tmpMax) {//最长后缀
                                max = tmpMax;
                                markPosOne = k;
                                markPosTwo = n;
                            }
                            k++;
                            continue;

                        }
                    }
                    if (k > 0) {
                        String a = Stream.of(Arrays.copyOfRange(oneTokenList, markPosOne, oneTokenList.length)).collect(Collectors.joining(" "));
                        String b = Stream.of(Arrays.copyOfRange(twoTokenList, markPosTwo, twoTokenList.length)).collect(Collectors.joining(" "));
                        if (b.equals("") || a.equals("")) {
                        } else {
                            if (isIn(a, b)) {
                                pos = oneTokenList.length - markPosOne - 1;
                            } else {
//                               System.out.println("可能不支持这个条件:"+one);
//                               System.out.println("可能不支持这个条件:"+two);
//                               pos =oneTokenList.length- markPosOne-1;
                            }
                        }
                    }

                    int finalPos = pos;
                    backtrackingTable.merge(one, finalPos, (oldv, newv) -> {
                        if (newv > oldv) {
                            return newv;
                        } else {
                            return oldv;
                        }
                    });
                    if (one.length() <= two.length()) {
                        if (isIn(one, two)) {
                            relationTable.compute(two, (s, list1) -> {
                                if (list1 == null) {
                                    list1 = new HashSet<>();
                                    list1.add(s);
                                    list1.add(one);
                                    return list1;
                                } else {
                                    list1.add(one);
                                    return list1;
                                }
                            });
                            continue;
                        }
                    }
                    Set<String> set = new HashSet<>();
                    set.add(two);
                    relationTable.computeIfAbsent(two, (s) -> set);
                    continue;
                }
            }
        }
        Map<Integer, String> int2str = new HashMap<>();
        Map<String, Integer> str2Int = new HashMap<>();
        Iterator<String> it = relationTable.keySet().iterator();
        for (int i = 1; it.hasNext(); i++) {
            String key = it.next();
            int2str.put(i, key);
            str2Int.put(key, i);
        }
        runtime.setInt2str(int2str);
        runtime.setMap(relationTable);
        runtime.setStr2Int(str2Int);
        runtime.setBacktrackingTable(backtrackingTable);
        return runtime;
    }

    public static boolean isIn(String f, String s) {
        f = f.replace("?", "([a-z0-9A-Z_$]|\\?|\"*\")");
        Matcher matcher = Pattern.compile(f).matcher(s);
        return matcher.find();
    }

    public static void insert(String str, TrieCompiler trieCompiler, String mark, int backPos) {
        BufferSQLParser parser = new BufferSQLParser();
        BufferSQLContext context = new BufferSQLContext();
        parser.parse(str.getBytes(), context);
        TrieCompiler.insertNode(context, trieCompiler, mark, backPos);
    }


}
