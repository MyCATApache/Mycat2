/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package cn.lightfish.pattern;

import org.reflections.Reflections;

import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public class DynamicSQLMatcherBuilder {
    private static long id = 0;
    private final String dafaultSchema;
    private final DynamicMatcherInfoBuilder dynamicMatcherInfoBuilder = new DynamicMatcherInfoBuilder();
    private final GPatternBuilder patternBuilder = new GPatternBuilder(0);
    private final DynamicMatcherInfoBuilder.PatternComplier patternComplier = pettern -> patternBuilder.addRule(pettern);
    private HashMap<Integer, List<Item>> runtimeMap;
    private HashMap<Set<SchemaTable>, SchemaItem> runtimeMap2;
    private TableCollectorBuilder tableCollctorbuilder;
    private Instruction defaultInstruction;

    public DynamicSQLMatcherBuilder(String dafaultSchema) {
        if (dafaultSchema != null) {
            this.dafaultSchema = dafaultSchema.toUpperCase();
        } else {
            this.dafaultSchema = null;
        }
    }

    public void addSchema(String schema, String pattern, String code) {
        dynamicMatcherInfoBuilder.addSchema(schema, pattern, code);
    }

    public void add(String pettern, String code) {
        dynamicMatcherInfoBuilder.add(pettern, code);
    }

    public void build(String packageName, boolean debug) throws Exception {
        build("java.util.Map", packageName, debug);
    }

    public void build(String contextClassName, String packageName, boolean debug) throws Exception {
        build(contextClassName, Collections.emptyList(), Collections.singletonList(packageName), null, null, debug);
    }

    public void build(String contextClassName, List<String> codeList, List<String> packageNameList, String schemaName, String defaultCode, boolean debug) throws Exception {
        packageNameList = packageNameList.stream().distinct().collect(Collectors.toList());
        dynamicMatcherInfoBuilder.build(patternComplier);
        this.runtimeMap = dynamicMatcherInfoBuilder.ruleInstructionMap;
        this.runtimeMap2 = dynamicMatcherInfoBuilder.tableInstructionMap;
        ArrayList<List<Item>> list = new ArrayList<>(runtimeMap.values());
        list.add(new ArrayList<>(this.runtimeMap2.values()));
        Reflections reflections = new Reflections(packageNameList);
        AddMehodClassFactory addMehodClassFactory = new AddMehodClassFactory(Instruction.class.getSimpleName() + id++, Instruction.class);
        addMehodClassFactory.addExpender(reflections, InstructionSet.class);
        for (List<Item> value : list) {
            for (Item item : value) {
                String name = Instruction.class.getSimpleName() + id++;
                String code = item.getCode();
                if (!code.endsWith(";")) {
                    code = ("return " + code + " ;");
                }
                addMehodClassFactory.implMethod("execute", MessageFormat.format("{0} ctx= ({0})$1;", contextClassName) +
                        "cn.lightfish.pattern.DynamicSQLMatcher matcher = (cn.lightfish.pattern.DynamicSQLMatcher)$2;", code);
                Class build = addMehodClassFactory.build(name, debug);
                Instruction o = (Instruction) build.newInstance();
                item.setInstruction(o);
            }
        }
        Map<String, Collection<String>> tableMap = dynamicMatcherInfoBuilder.getTableMap();
        if (schemaName != null) {
            String[] split = schemaName.split(",");
            for (String s : split) {
                String[] split1 = s.split("\\.");
                Collection<String> strings = tableMap.computeIfAbsent(split1[0], s1 -> new HashSet<>());
                strings.add(split1[1]);
            }
        }
        this.tableCollctorbuilder = new TableCollectorBuilder(patternBuilder.geIdRecorder(), tableMap);

        DynamicSQLMatcher matcher = createMatcher();

        StringBuilder sb = new StringBuilder();
        for (String line : codeList) {
            if (!line.trim().endsWith(";")) {
                line = line + ";";
            }
            sb.append(line);
        }
        sb.append(";return null;");
        String name = Instruction.class.getSimpleName() + id++;
        addMehodClassFactory = new AddMehodClassFactory(name, Runnable.class);
        addMehodClassFactory.addExpender(reflections, InstructionSet.class);
        addMehodClassFactory.implMethod("run", ";", sb.toString());
        Class initCode = addMehodClassFactory.build(debug);
        Runnable o = (Runnable) initCode.newInstance();
        o.run();
        //deafultCode

        AddMehodClassFactory mehodClassFactory = new AddMehodClassFactory(Instruction.class.getSimpleName() + id++, Instruction.class);
        mehodClassFactory.addExpender(reflections, InstructionSet.class);

        if (defaultCode != null) {
            String code = defaultCode;
            name = "DefaultCode";
            if (!code.endsWith(";")) {
                code = ("return " + code + " ;");
            }
            mehodClassFactory.implMethod("execute", MessageFormat.format("{0} ctx= ({0})$1;", contextClassName) +
                    "cn.lightfish.pattern.DynamicSQLMatcher matcher = (cn.lightfish.pattern.DynamicSQLMatcher)$2;", code);
            Class build = mehodClassFactory.build(name, debug);
            this.defaultInstruction = (Instruction) build.newInstance();
        }
    }

    public DynamicSQLMatcher createMatcher() {
        TableCollector tableCollector = tableCollctorbuilder.create();
        if (dafaultSchema != null) {
            tableCollector.useSchema(dafaultSchema);
        }
        GPattern gPattern = patternBuilder.createGroupPattern(tableCollector);
        return new DynamicSQLMatcher(tableCollector, gPattern, runtimeMap, runtimeMap2);
    }

    public Instruction getDefaultInstruction() {
        return defaultInstruction;
    }
}