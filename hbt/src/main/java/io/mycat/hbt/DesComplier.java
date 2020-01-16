/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.hbt;

import io.mycat.hbt.ast.base.Schema;
import org.apache.commons.io.FileUtils;
import org.codehaus.janino.JavaSourceClassLoader;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author jamie12221
 **/
public class DesComplier {
    static long id = 0;
    private static Path src;
    String complierDir = "codeSrc";
    String packageName = "Lj";
    String importClass = " static cn.lightfish.hbt.Ast.*";

    public DesComplier() {
        this(Paths.get("codeSrc").toAbsolutePath().toString(), "Lj", " static io.mycat.hbt.BaseQuery.* ");
    }

    public DesComplier(String complierDir, String packageName, String importClass) {
        try {
            src = Paths.get(complierDir);
            if (Files.exists(src)){
                FileUtils.forceDelete(src.toFile());
            }
            Files.createDirectories(src);
            src = Files.createDirectories(src.resolve(packageName));
            this.complierDir = complierDir;
            this.packageName = packageName;
            this.importClass = importClass;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static String nextName() {
        return "Lj" + id++;
    }

    public Schema complie(String returnText) throws Exception {
        return complie(returnText, nextName());
    }

    private Schema complie(String returnText, String className) throws Exception {
        List<String> importList = Arrays.asList(importClass, "java.util.function.Supplier");
        String sb = MessageFormat.format("package {0};\n", packageName) +
                importList.stream().collect(Collectors.joining(";import ", "import ", ";\n")) +
                MessageFormat.format("public class {0} implements {1}", className, "    Supplier") +
                "{ \npublic Object get(){" + "return \n" + returnText + ";" + "\n}\n}";
        Files.write(src.resolve(className + ".java"), sb.getBytes());
        ClassLoader cl = new JavaSourceClassLoader(
                BaseQuery.class.getClassLoader(),  // parentClassLoader
                new File[]{Paths.get(complierDir).toFile()}, // optionalSourcePath
                null);
        Class aClass = cl.loadClass(packageName + "." + className);
        Supplier supplier = (Supplier) aClass.newInstance();
        return (Schema) supplier.get();
    }

}