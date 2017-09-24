package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**

 */
public class DynamicClassLoader extends ClassLoader {

    private String classPath;

    public DynamicClassLoader(String classPath, ClassLoader parent) {
        super(parent);
        this.classPath = classPath;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        if (name != null && name.equals(classPath)) {
            return findClass(name);
        }
        return super.loadClass(name, false);
    }

    /**
     * 根据类名查找class
     *
     * @param fullClassPath
     * @throws ClassNotFoundException
     */
    @Override
    public Class<?> findClass(String fullClassPath)
            throws ClassNotFoundException {
        try {
            byte[] raw = readClassBytes(fullClassPath);
             //todo 解决这个过时方法
            Class<?> clazz = defineClass(raw, 0, raw.length);
            resolveClass(clazz);
            return clazz;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }

    }

    /**
     * 读取class
     *
     * @param fullClassPath
     * @return
     */
    private byte[] readClassBytes(String fullClassPath) throws IOException{
       return  Files.readAllBytes(Paths.get(fullClassPath));
    }
}