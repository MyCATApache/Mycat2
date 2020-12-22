package io.mycat.util;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.annotation.*;
import java.lang.reflect.*;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.annotation.ElementType.*;

/**
 * Lightweight container that supports resource injection
 *
 * @author wangzihao
 * 2016/11/11/011
 */
public class ApplicationContext {
    private static final AtomicInteger SHUTDOWN_HOOK_ID_INCR = new AtomicInteger();
    private static final Method[] EMPTY_METHOD_ARRAY = {};
    private static final PropertyDescriptor[] EMPTY_DESCRIPTOR_ARRAY = {};
    private static final Constructor<ConcurrentMap> CONCURRENT_REFERENCE_MAP_CONSTRUCTOR = getAnyConstructor(
            new Class[]{int.class},
            "com.github.netty.core.util.ConcurrentReferenceHashMap",
            "org.springframework.util.ConcurrentReferenceHashMap",
            "org.hibernate.validator.internal.util.ConcurrentReferenceHashMap"
    );

    private static final Map<Class, Boolean> AUTOWIRED_ANNOTATION_CACHE_MAP = newConcurrentReferenceMap(128);
    private static final Map<Class, Boolean> QUALIFIER_ANNOTATION_CACHE_MAP = newConcurrentReferenceMap(128);
    private static final Map<Class, Boolean> FACTORY_METHOD_ANNOTATION_CACHE_MAP = newConcurrentReferenceMap(32);
    private static final Map<Class, PropertyDescriptor[]> PROPERTY_DESCRIPTOR_CACHE_MAP = newConcurrentReferenceMap(128);
    private static final Map<Class, Method[]> DECLARED_METHODS_CACHE_MAP = newConcurrentReferenceMap(128);
    private final Collection<Class<? extends Annotation>> initMethodAnnotations = new LinkedHashSet<>(
            Arrays.asList(PostConstruct.class));
    private final Collection<Class<? extends Annotation>> destroyMethodAnnotations = new LinkedHashSet<>(
            Arrays.asList(PreDestroy.class));
    private final Collection<Class<? extends Annotation>> scannerAnnotations = new LinkedHashSet<>(
            Arrays.asList(Resource.class, Component.class));
    private final Collection<Class<? extends Annotation>> autowiredAnnotations = new LinkedHashSet<>(
            Arrays.asList(Resource.class, Autowired.class));
    private final Collection<Class<? extends Annotation>> qualifierAnnotations = new LinkedHashSet<>(
            Arrays.asList(Resource.class, Qualifier.class));
    private final Collection<Class<? extends Annotation>> orderedAnnotations = new LinkedHashSet<>(
            Arrays.asList(Order.class));
    private final Collection<Class<? extends Annotation>> factoryMethodAnnotations = new LinkedHashSet<>(
            Arrays.asList(Bean.class));
    //BeanPostProcessor接口是为了将每个bean的处理阶段的处理, 抽象成接口, 让用户可以根据不同需求不同处理. 比如自动注入,AOP,定时任务,异步注解,servlet注入,错误页注册
    private final Collection<BeanPostProcessor> beanPostProcessors = new TreeSet<>(new OrderComparator(orderedAnnotations));
    //需要跳过生命周期管理的bean名称集合
    private final Collection<String> beanSkipLifecycles = new LinkedHashSet<>(8);
    //存放Class与bean名称对应关系
    private final Map<Class, String[]> beanNameMap = new ConcurrentHashMap<>(64);
    //存放别名与别名关系或别名与bean名称的关系
    private final Map<String, String> beanAliasMap = new ConcurrentHashMap<>(6);
    //存放bean名称与bean描述的关系
    private final Map<String, BeanDefinition> beanDefinitionMap = new ConcurrentHashMap<>(64);
    //存放bean名称与单例对象的关系
    private final Map<String, Object> beanInstanceMap = new ConcurrentHashMap<>(64);
    private final Map<Class, AbstractBeanFactory> beanFactoryMap = new LinkedHashMap<>(8);
    private final AbstractBeanFactory defaultBeanFactory = new DefaultBeanFactory();
    private final Scanner scanner = new Scanner();
    private Supplier<ClassLoader> resourceLoader;
    private Function<BeanDefinition, String> beanNameGenerator = new DefaultBeanNameGenerator(this);

    public ApplicationContext() {
        this(ApplicationContext.class::getClassLoader);
    }

    public ApplicationContext(Supplier<ClassLoader> resourceLoader) {
        this.resourceLoader = Objects.requireNonNull(resourceLoader);
        addClasses(initMethodAnnotations,
                "javax.annotation.PostConstruct");
        addClasses(destroyMethodAnnotations,
                "javax.annotation.PreDestroy");
        addClasses(scannerAnnotations,
                "javax.annotation.Resource",
                "org.springframework.stereotype.Component");
        addClasses(autowiredAnnotations,
                "javax.annotation.Resource",
                "javax.inject.Inject",
                "org.springframework.beans.factory.annotation.Autowired");
        addClasses(qualifierAnnotations,
                "javax.annotation.Resource",
                "org.springframework.beans.factory.annotation.Qualifier");
        addClasses(orderedAnnotations,
                "org.springframework.core.annotation.Order");
        addInstance(this);
        addBeanPostProcessor(new RegisteredBeanPostProcessor(this));
        addBeanPostProcessor(new AutowiredConstructorPostProcessor(this));
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownHook, "app.shutdownHook-" + SHUTDOWN_HOOK_ID_INCR.getAndIncrement()));
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        ApplicationContext app = new ApplicationContext();
        System.out.println("new = " + (System.currentTimeMillis() - startTime) + "/ms");

        startTime = System.currentTimeMillis();
        int count = app.scanner("com.github.netty").inject();
        System.out.println("scanner = " + (System.currentTimeMillis() - startTime) + "/ms");

        System.out.println("count = " + count);
        System.out.println("app = " + app);
    }

    private static boolean isAbstract(Class clazz) {
        int modifier = clazz.getModifiers();
        return Modifier.isInterface(modifier) || Modifier.isAbstract(modifier);
    }

    private static Boolean isExistAnnotation0(Class clazz, Collection<Class<? extends Annotation>> finds, Map<Class, Boolean> cacheMap) {
        Annotation annotation;
        Boolean exist = cacheMap.get(clazz);
        if (finds.contains(clazz)) {
            exist = Boolean.TRUE;
        } else if (exist == null) {
            exist = Boolean.FALSE;
            cacheMap.put(clazz, exist);
            Queue<Annotation> queue = new LinkedList<>(Arrays.asList(clazz.getDeclaredAnnotations()));
            while ((annotation = queue.poll()) != null) {
                Class<? extends Annotation> annotationType = annotation.annotationType();
                if (annotationType == clazz) {
                    continue;
                }
                if (finds.contains(annotationType)) {
                    exist = Boolean.TRUE;
                    break;
                }
                if (isExistAnnotation0(annotationType, finds, cacheMap)) {
                    exist = Boolean.TRUE;
                    break;
                }
            }
        }
        cacheMap.put(clazz, exist);
        return exist;
    }

    private static boolean isExistAnnotation(Class clazz, Collection<Class<? extends Annotation>> finds, Map<Class, Boolean> cacheMap) {
        Boolean existAnnotation = cacheMap.get(clazz);
        if (existAnnotation == null) {
            Map<Class, Boolean> tempCacheMap = new HashMap<>();
            existAnnotation = isExistAnnotation0(clazz, finds, tempCacheMap);
            cacheMap.putAll(tempCacheMap);
        }
        return existAnnotation;
    }

    private static <T> T getAnnotationValue(Annotation annotation, String[] fieldNames, Class<T> type) {
        for (String fieldName : fieldNames) {
            T value = getAnnotationValue(annotation, fieldName, type);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private static <T> T getAnnotationValue(Annotation annotation, String fieldName, Class<T> type) {
        try {
            Method method = annotation.annotationType().getDeclaredMethod(fieldName);
            Object value = method.invoke(annotation);
            if (value != null && type.isAssignableFrom(value.getClass())) {
                return (T) value;
            } else {
                return null;
            }
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            return null;
        }
    }

    private static Annotation findDeclaredAnnotation(AnnotatedElement element, Collection<Class<? extends Annotation>> finds, Map<Class, Boolean> cacheMap) {
        Annotation[] fieldAnnotations = element.getDeclaredAnnotations();
        for (Annotation annotation : fieldAnnotations) {
            boolean existAnnotation = isExistAnnotation(annotation.annotationType(), finds, cacheMap);
            if (existAnnotation) {
                return annotation;
            }
        }
        return null;
    }

    private static Annotation findAnnotation(Class rootClass, Collection<Class<? extends Annotation>> finds) {
        if (rootClass == null) {
            return null;
        }
        Annotation result;
        //类上找
        for (Class clazz = rootClass; clazz != null && clazz != Object.class; clazz = clazz.getSuperclass()) {
            for (Class<? extends Annotation> find : finds) {
                if (null != (result = clazz.getAnnotation(find))) {
                    return result;
                }
            }
        }
        //接口上找
        Collection<Class> interfaces = getInterfaces(rootClass);
        for (Class i : interfaces) {
            for (Class clazz = i; clazz != null; clazz = clazz.getSuperclass()) {
                for (Class<? extends Annotation> find : finds) {
                    if (null != (result = clazz.getAnnotation(find))) {
                        return result;
                    }
                }
            }
        }
        return null;
    }

    private static Collection<Class> getInterfaces(Class sourceClass) {
        Set<Class> interfaceList = new LinkedHashSet<>();
        if (sourceClass.isInterface()) {
            interfaceList.add(sourceClass);
        }
        for (Class currClass = sourceClass; currClass != null && currClass != Object.class; currClass = currClass.getSuperclass()) {
            Collections.addAll(interfaceList, currClass.getInterfaces());
        }
        return interfaceList;
    }

    private static boolean isSimpleProperty(Class<?> clazz) {
        return isSimpleValueType(clazz) || (clazz.isArray() && isSimpleValueType(clazz.getComponentType()));
    }

    private static boolean isSimpleValueType(Class<?> clazz) {
        return (clazz.isPrimitive() ||
                clazz == Character.class ||
                Enum.class.isAssignableFrom(clazz) ||
                CharSequence.class.isAssignableFrom(clazz) ||
                Number.class.isAssignableFrom(clazz) ||
                Date.class.isAssignableFrom(clazz) ||
                URI.class == clazz || URL.class == clazz ||
                Locale.class == clazz || Class.class == clazz);
    }

    private static String findMethodNameByNoArgs(Class clazz, Collection<Class<? extends Annotation>> methodAnnotations) {
        for (Method method : clazz.getMethods()) {
            if (method.getDeclaringClass() == Object.class
                    || method.getReturnType() != void.class
                    || method.getParameterCount() != 0) {
                continue;
            }
            for (Class<? extends Annotation> aClass : methodAnnotations) {
                if (method.getAnnotationsByType(aClass).length == 0) {
                    continue;
                }
                if (method.getParameterCount() != 0) {
                    throw new IllegalStateException("method does not have parameters. class=" + clazz + ",method=" + method);
                }
                return method.getName();
            }
        }
        return null;
    }

    private static PropertyDescriptor[] getPropertyDescriptorsIfCache(Class clazz) throws IllegalStateException {
        PropertyDescriptor[] result = PROPERTY_DESCRIPTOR_CACHE_MAP.get(clazz);
        if (result == null) {
            try {
                BeanInfo beanInfo = Introspector.getBeanInfo(clazz, Object.class, Introspector.USE_ALL_BEANINFO);
                PropertyDescriptor[] descriptors = beanInfo.getPropertyDescriptors();
                if (descriptors != null) {
                    result = descriptors;
                } else {
                    result = EMPTY_DESCRIPTOR_ARRAY;
                }
                PROPERTY_DESCRIPTOR_CACHE_MAP.put(clazz, result);
            } catch (IntrospectionException e) {
                throw new IllegalStateException("getPropertyDescriptors error. class=" + clazz + e, e);
            }
            // TODO: 1月28日 028 getPropertyDescriptorsIfCache
            // skip GenericTypeAwarePropertyDescriptor leniently resolves a set* write method
            // against a declared read method, so we prefer read method descriptors here.
        }
        return result;
    }

    private static <K, V> ConcurrentMap<K, V> newConcurrentReferenceMap(int initialCapacity) {
        if (CONCURRENT_REFERENCE_MAP_CONSTRUCTOR != null) {
            try {
                return CONCURRENT_REFERENCE_MAP_CONSTRUCTOR.newInstance(initialCapacity);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                //skip
            }
        }
        return new ConcurrentHashMap<>(initialCapacity);
    }

    private static Method[] getDeclaredMethods(Class<?> clazz) {
        Objects.requireNonNull(clazz);
        Method[] result = DECLARED_METHODS_CACHE_MAP.get(clazz);
        if (result == null) {
            try {
                Method[] declaredMethods = clazz.getDeclaredMethods();
                List<Method> defaultMethods = findConcreteMethodsOnInterfaces(clazz);
                if (defaultMethods != null) {
                    result = new Method[declaredMethods.length + defaultMethods.size()];
                    System.arraycopy(declaredMethods, 0, result, 0, declaredMethods.length);
                    int index = declaredMethods.length;
                    for (Method defaultMethod : defaultMethods) {
                        result[index] = defaultMethod;
                        index++;
                    }
                } else {
                    result = declaredMethods;
                }
                DECLARED_METHODS_CACHE_MAP.put(clazz, (result.length == 0 ? EMPTY_METHOD_ARRAY : result));
            } catch (Throwable ex) {
                throw new IllegalStateException("Failed to introspect Class [" + clazz.getName() +
                        "] from ClassLoader [" + clazz.getClassLoader() + "]", ex);
            }
        }
        return result;
    }

    private static List<Method> findConcreteMethodsOnInterfaces(Class<?> clazz) {
        List<Method> result = null;
        for (Class<?> ifc : clazz.getInterfaces()) {
            for (Method ifcMethod : ifc.getMethods()) {
                if (!Modifier.isAbstract(ifcMethod.getModifiers())) {
                    if (result == null) {
                        result = new ArrayList<>();
                    }
                    result.add(ifcMethod);
                }
            }
        }
        return result;
    }

    private static void eachClass(Class<?> clazz, Consumer<Class> consumer) {
        // Keep backing up the inheritance hierarchy.
        consumer.accept(clazz);
        Class<?> superclass = clazz.getSuperclass();
        if (superclass != null && superclass != Object.class) {
            eachClass(superclass, consumer);
        } else if (clazz.isInterface()) {
            for (Class<?> superIfc : clazz.getInterfaces()) {
                eachClass(superIfc, consumer);
            }
        }
    }

    private static <T> Constructor<T> getAnyConstructor(Class<?>[] parameterTypes, String... referenceMaps) {
        for (String s : referenceMaps) {
            try {
                Class<T> aClass = (Class<T>) Class.forName(s);
                return aClass.getDeclaredConstructor(parameterTypes);
            } catch (ClassNotFoundException | NoSuchMethodException e) {
                //skip
            }
        }
        return null;
    }

    private void addClasses(Collection annotationList, String... classNames) {
        ClassLoader classLoader = resourceLoader.get();
        for (String className : classNames) {
            try {
                annotationList.add(Class.forName(className, false, classLoader));
            } catch (Exception e) {
                //skip
            }
        }
    }

    public Object addInstance(Object instance) {
        return addInstance(instance, null, true, null);
    }

    public Object addInstance(Object instance, String beanName) {
        return addInstance(instance, beanName, true, null);
    }

    public Object addInstance(Object instance, String beanName, boolean isLifecycle) {
        return addInstance(instance, beanName, isLifecycle, null);
    }

    public Object addInstance(Object instance, String beanName, boolean isLifecycle, BiConsumer<String, BeanDefinition> beanDefinitionConfig) {
        Class beanType = instance.getClass();
        BeanDefinition definition = newBeanDefinition(beanType);
        definition.setBeanSupplier(() -> instance);
        if (!isLifecycle) {
            beanSkipLifecycles.add(beanName);
        }
        if (beanName == null) {
            beanName = beanNameGenerator.apply(definition);
        }
        if (beanDefinitionConfig != null) {
            beanDefinitionConfig.accept(beanName, definition);
        }
        addBeanDefinition(beanName, definition);
        Object oldInstance = beanInstanceMap.remove(beanName, instance);
        if (!definition.isLazyInit()) {
            getBean(beanName, null, true);
        }
        return oldInstance;
    }

    /**
     * 注册别名
     *
     * @param name  bean名称
     * @param alias 别名
     */
    public void registerAlias(String name, String alias) {
        Objects.requireNonNull(name, "'name' must not be empty");
        Objects.requireNonNull(alias, "'alias' must not be empty");
        synchronized (this.beanAliasMap) {
            if (alias.equals(name)) {
                this.beanAliasMap.remove(alias);
            } else {
                String registeredName = this.beanAliasMap.get(alias);
                if (registeredName != null && registeredName.equals(name)) {
                    // An existing alias - no need to re-register
                    return;
                }
                if (hasAlias(alias, name)) {
                    throw new IllegalStateException("Cannot register alias '" + alias +
                            "' for name '" + name + "': Circular reference - '" +
                            name + "' is a direct or indirect alias for '" + alias + "' already");
                }
                this.beanAliasMap.put(alias, name);
            }
        }
    }

    public boolean hasAlias(String name, String alias) {
        for (Map.Entry<String, String> entry : this.beanAliasMap.entrySet()) {
            String registeredName = entry.getValue();
            if (registeredName.equals(name)) {
                String registeredAlias = entry.getKey();
                if (registeredAlias.equals(alias) || hasAlias(registeredAlias, alias)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void removeAlias(String alias) {
        synchronized (this.beanAliasMap) {
            String name = this.beanAliasMap.remove(alias);
            if (name == null) {
                throw new IllegalStateException("No alias '" + alias + "' registered");
            }
        }
    }

    public boolean isAlias(String name) {
        return this.beanAliasMap.containsKey(name);
    }

    /**
     * 确定原始名称，解析别名规范名称。
     *
     * @param beanNameOrAlias 用户指定的名称
     * @return beanName
     */
    public String getBeanName(String beanNameOrAlias) {
        String canonicalName = beanNameOrAlias;
        // Handle aliasing...
        String resolvedName;
        do {
            resolvedName = this.beanAliasMap.get(canonicalName);
            if (resolvedName != null) {
                canonicalName = resolvedName;
            }
        } while (resolvedName != null);
        return canonicalName;
    }

    public String[] getAliases(String name) {
        List<String> result = new ArrayList<>();
        synchronized (this.beanAliasMap) {
            retrieveAliases(name, result);
        }
        return result.toArray(new String[0]);
    }

    private void retrieveAliases(String name, List<String> result) {
        for (Map.Entry<String, String> entry : beanAliasMap.entrySet()) {
            String alias = entry.getKey();
            String registeredName = entry.getValue();
            if (registeredName.equals(name)) {
                result.add(alias);
                retrieveAliases(alias, result);
            }
        }
    }

    private BiConsumer<URL, String> newScannerConsumer(ClassLoader classLoader, ScannerResult result) {
        return (url, className) -> {
            try {
                result.classCount.incrementAndGet();
                Class clazz = Class.forName(className, false, classLoader);
                if (clazz.isAnnotation()) {
                    return;
                }
                // TODO: 1月27日 027  doScan skip interface impl by BeanPostProcessor
                if (clazz.isInterface()) {
                    return;
                }
                if (!isExistAnnotation(clazz, scannerAnnotations, result.scannerAnnotationCacheMap)) {
                    return;
                }
                BeanDefinition definition = newBeanDefinition(clazz);
                String beanName = beanNameGenerator.apply(definition);
                result.beanDefinitionMap.put(beanName, definition);
            } catch (ReflectiveOperationException | LinkageError e) {
                //skip
            }
        };
    }

    public ScannerResult scanner(ClassLoader classLoader, boolean onlyInMyProject) {
        ScannerResult result = new ScannerResult();
        ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
        //只在我的项目中搜索类
        if (onlyInMyProject) {
            result.classLoaderList.add(classLoader);
            BiConsumer<URL, String> consumer = newScannerConsumer(classLoader, result);
            try {
                for (String rootPackage : scanner.getRootPackages()) {
                    scanner.doScan(rootPackage, classLoader, consumer);
                }
            } catch (IOException e) {
                throw new IllegalStateException("scanner classLoader=" + classLoader + ",error=" + e, e);
            }
            return result;
        }

        //获取用户自定义路径url
        for (ClassLoader userLoader = classLoader; userLoader != null && userLoader != systemClassLoader; userLoader = userLoader.getParent()) {
            if (!(userLoader instanceof URLClassLoader)) {
                continue;
            }
            URL[] urls = ((URLClassLoader) userLoader).getURLs();
            if (urls == null) {
                continue;
            }
            result.classUrls.addAll(Arrays.asList(urls));
            result.classLoaderList.add(userLoader);
        }

        //扫描所有用户自定义加载器jar包路径
        for (URL url : result.classUrls) {
            BiConsumer<URL, String> consumer = newScannerConsumer(classLoader, result);
            try {
                for (String rootPackage : scanner.getRootPackages()) {
                    scanner.doScan(rootPackage, null, url, consumer);
                }
            } catch (IOException e) {
                throw new IllegalStateException("scanner userClassLoader error. url=" + url + ",error=" + e, e);
            }
        }

        //扫描系统类加载器的jar包路径
        if (systemClassLoader instanceof URLClassLoader) {
            URL[] urls = ((URLClassLoader) systemClassLoader).getURLs();
            if (urls != null) {
                result.classUrls.addAll(Arrays.asList(urls));
                result.classLoaderList.add(systemClassLoader);
                BiConsumer<URL, String> consumer = newScannerConsumer(systemClassLoader, result);
                for (URL url : urls) {
                    try {
                        for (String rootPackage : scanner.getRootPackages()) {
                            scanner.doScan(rootPackage, null, url, consumer);
                        }
                    } catch (IOException e) {
                        throw new IllegalStateException("scanner systemClassLoader error. url=" + url + ",error=" + e, e);
                    }
                }
            }
        }
        return result;
    }

    public ScannerResult scanner(String... rootPackage) {
        return scanner(false, rootPackage);
    }

    public ScannerResult scanner(boolean onlyInMyProject, String... rootPackage) {
        addScanPackage(rootPackage);
        ClassLoader loader = resourceLoader.get();
        return scanner(loader, onlyInMyProject);
    }

    public ApplicationContext addExcludesPackage(String... excludesPackages) {
        if (excludesPackages != null) {
            scanner.getExcludes().addAll(Arrays.asList(excludesPackages));
        }
        return this;
    }

    public ApplicationContext addScanPackage(String... rootPackages) {
        if (rootPackages != null) {
            scanner.getRootPackages().addAll(Arrays.asList(rootPackages));
        }
        return this;
    }

    public ApplicationContext removeScanPackage(String... rootPackages) {
        if (rootPackages != null) {
            scanner.getRootPackages().removeAll(Arrays.asList(rootPackages));
        }
        return this;
    }

    public ApplicationContext addBeanPostProcessor(BeanPostProcessor beanPostProcessor) {
        beanPostProcessors.add(beanPostProcessor);
        return this;
    }

    public ApplicationContext addBeanFactory(Class type, AbstractBeanFactory beanFactory) {
        addInstance(beanFactory);
        beanFactoryMap.put(type, beanFactory);
        return this;
    }

    public BeanDefinition[] getBeanDefinitions(Class clazz) {
        String[] beanNames = beanNameMap.get(clazz);
        BeanDefinition[] beanDefinitions = new BeanDefinition[beanNames.length];
        for (int i = 0; i < beanNames.length; i++) {
            beanDefinitions[i] = getBeanDefinition(beanNames[i]);
        }
        return beanDefinitions;
    }

    public BeanDefinition getBeanDefinition(String beanName) {
        BeanDefinition definition = beanDefinitionMap.get(beanName);
        return definition;
    }

    public String[] getBeanNamesForType(Class clazz) {
        Collection<String> result = new ArrayList<>();
        for (Map.Entry<String, BeanDefinition> entry : beanDefinitionMap.entrySet()) {
            BeanDefinition definition = entry.getValue();
            if (clazz.isAssignableFrom(definition.getBeanClassIfResolve(resourceLoader))) {
                String beanName = entry.getKey();
                result.add(beanName);
            }
        }
        return result.toArray(new String[0]);
    }

    public <T> T getBean(Class<T> clazz) {
        return getBean(clazz, null, true);
    }

    public <T> T getBean(Class<T> clazz, Object[] args, boolean required) {
        String[] beanNames = getBeanNamesForType(clazz);
        String beanName;
        if (beanNames.length == 0) {
            if (required) {
                throw new IllegalStateException("Not found bean. by type=" + clazz);
            } else {
                return null;
            }
        } else if (beanNames.length == 1) {
            beanName = beanNames[0];
        } else {
            List<String> primaryBeanNameList = new ArrayList<>(beanNames.length);
            List<String> nonPrimaryBeanNameList = new ArrayList<>(beanNames.length);
            for (String eachBeanName : beanNames) {
                BeanDefinition definition = getBeanDefinition(eachBeanName);
                if (definition.isPrimary()) {
                    primaryBeanNameList.add(eachBeanName);
                } else {
                    nonPrimaryBeanNameList.add(eachBeanName);
                }
            }
            if (primaryBeanNameList.isEmpty()) {
                if (nonPrimaryBeanNameList.size() == 1) {
                    beanName = nonPrimaryBeanNameList.get(0);
                } else {
                    throw new IllegalStateException("Found more bean. you can Annotation @Primary. beanNames=" + nonPrimaryBeanNameList);
                }
            } else if (primaryBeanNameList.size() == 1) {
                beanName = primaryBeanNameList.get(0);
            } else {
                throw new IllegalStateException("Found more primary bean. beanNames=" + primaryBeanNameList);
            }
        }
        return getBean(beanName, args, required);
    }

    public <T> T getBean(String beanNameOrAlias, Object[] args, boolean required) {
        String beanName = getBeanName(beanNameOrAlias);
        BeanDefinition definition = beanDefinitionMap.get(beanName);
        if (definition == null) {
            if (required) {
                throw new IllegalStateException("getBean error. bean is not definition. beanName=" + beanName);
            } else {
                return null;
            }
        }
        Object instance = definition.isSingleton() ? beanInstanceMap.get(beanName) : null;
        if (instance == null) {
            Class beanClass = definition.getBeanClassIfResolve(resourceLoader);
            AbstractBeanFactory beanFactory = getBeanFactory(beanClass);
            instance = beanFactory.createBean(beanName, definition, args);
        }
        if (definition.isSingleton()) {
            beanInstanceMap.put(beanName, instance);
        }
        return (T) instance;
    }

    public <T> T getBean(String beanName) {
        return (T) getBean(beanName, null, true);
    }

    public <T> T getBean(String beanName, Object[] args) {
        return (T) getBean(beanName, args, true);
    }

    public <T> List<T> getBeanForAnnotation(Class<? extends Annotation>... annotationType) {
        List<T> result = new ArrayList<>();
        for (Map.Entry<String, BeanDefinition> entry : beanDefinitionMap.entrySet()) {
            String beanName = entry.getKey();
            BeanDefinition definition = entry.getValue();
            Class beanClass = definition.getBeanClassIfResolve(resourceLoader);
            Annotation annotation = findAnnotation(beanClass, Arrays.asList(annotationType));
            if (annotation != null) {
                T bean = getBean(beanName, null, false);
                if (bean != null) {
                    result.add(bean);
                }
            }
        }
        return result;
    }

    public <T> List<T> getBeanForType(Class<T> clazz) {
        List<T> result = new ArrayList<>();
        for (String beanName : getBeanNamesForType(clazz)) {
            T bean = getBean(beanName, null, false);
            if (bean != null) {
                result.add(bean);
            }
        }
        return result;
    }

    public boolean containsBean(String name) {
        String beanName = getBeanName(name);
        return beanInstanceMap.containsKey(beanName) || beanDefinitionMap.containsKey(beanName);
    }

    public boolean containsInstance(String name) {
        String beanName = getBeanName(name);
        return beanInstanceMap.containsKey(beanName);
    }

    public BeanDefinition newBeanDefinition(Class beanType) {
        return newBeanDefinition(beanType, beanType);
    }

    public BeanDefinition newBeanDefinition(Class beanType, AnnotatedElement annotatedElement) {
        Lazy lazyAnnotation = annotatedElement.getAnnotation(Lazy.class);
        Scope scopeAnnotation = annotatedElement.getAnnotation(Scope.class);
        Primary primaryAnnotation = annotatedElement.getAnnotation(Primary.class);

        BeanDefinition definition = new BeanDefinition();
        definition.setBeanClass(beanType);
        definition.setBeanClassName(beanType.getName());
        definition.setScope(scopeAnnotation == null ? BeanDefinition.SCOPE_SINGLETON : scopeAnnotation.value());
        definition.setLazyInit(lazyAnnotation != null && lazyAnnotation.value());
        definition.setInitMethodName(findMethodNameByNoArgs(beanType, initMethodAnnotations));
        definition.setDestroyMethodName(findMethodNameByNoArgs(beanType, destroyMethodAnnotations));
        definition.setPrimary(primaryAnnotation != null);
        return definition;
    }

    public BeanDefinition addBeanDefinition(String beanName, BeanDefinition definition) {
        return addBeanDefinition(beanName, definition, beanNameMap, beanDefinitionMap);
    }

    public BeanDefinition addBeanDefinition(String beanName, BeanDefinition definition,
                                            Map<Class, String[]> beanNameMap,
                                            Map<String, BeanDefinition> beanDefinitionMap) {
        Class beanClass = definition.getBeanClassIfResolve(resourceLoader);
        String[] oldBeanNames = beanNameMap.get(beanClass);
        Set<String> nameSet = oldBeanNames != null ? new LinkedHashSet<>(Arrays.asList(oldBeanNames)) : new LinkedHashSet<>(1);
        nameSet.add(beanName);

        beanNameMap.put(beanClass, nameSet.toArray(new String[0]));
        return beanDefinitionMap.put(beanName, definition);
    }

    protected int findAutowireType(AnnotatedElement field) {
        int autowireType;
        Annotation qualifierAnnotation = findDeclaredAnnotation(field, qualifierAnnotations, QUALIFIER_ANNOTATION_CACHE_MAP);
        if (qualifierAnnotation != null) {
            if (Objects.equals(Resource.class.getSimpleName(), qualifierAnnotation.annotationType().getSimpleName())) {
                String autowiredBeanName = getAnnotationValue(qualifierAnnotation, "name", String.class);
                autowireType = (autowiredBeanName == null || autowiredBeanName.isEmpty()) ?
                        BeanDefinition.AUTOWIRE_BY_TYPE : BeanDefinition.AUTOWIRE_BY_NAME;
            } else {
                autowireType = BeanDefinition.AUTOWIRE_BY_NAME;
            }
        } else {
            autowireType = BeanDefinition.AUTOWIRE_BY_TYPE;
        }
        return autowireType;
    }

    protected Object initializeBean(String beanName, BeanWrapper beanWrapper, BeanDefinition definition) throws IllegalStateException {
        Object bean = beanWrapper.getWrappedInstance();
        invokeBeanAwareMethods(beanName, bean, definition);
        Object wrappedBean = bean;
        wrappedBean = applyBeanBeforeInitialization(beanName, wrappedBean);
        invokeBeanInitialization(beanName, bean, definition);
        wrappedBean = applyBeanAfterInitialization(beanName, wrappedBean);
        return wrappedBean;
    }

    protected void invokeBeanAwareMethods(String beanName, Object bean, BeanDefinition definition) throws IllegalStateException {
        if (bean instanceof Aware) {
            if (bean instanceof BeanNameAware) {
                ((BeanNameAware) bean).setBeanName(beanName);
            }
            if (bean instanceof ApplicationAware) {
                ((ApplicationAware) bean).setApplication(this);
            }
        }
    }

    protected Object applyBeanBeforeInitialization(String beanName, Object bean) throws IllegalStateException {
        Object result = bean;
        for (BeanPostProcessor processor : new ArrayList<>(beanPostProcessors)) {
            Object current;
            try {
                current = processor.postProcessBeforeInitialization(result, beanName);
            } catch (Exception e) {
                throw new IllegalStateException("applyBeanBeforeInitialization error=" + e, e);
            }
            if (current == null) {
                return result;
            }
            result = current;
        }
        return result;
    }

    private Object applyBeanAfterInitialization(String beanName, Object bean) throws IllegalStateException {
        Object result = bean;
        for (BeanPostProcessor processor : new ArrayList<>(beanPostProcessors)) {
            Object current;
            try {
                current = processor.postProcessAfterInitialization(result, beanName);
            } catch (Exception e) {
                throw new IllegalStateException("applyBeanAfterInitialization error=" + e, e);
            }
            if (current == null) {
                return result;
            }
            result = current;
        }
        return result;
    }

    private AbstractBeanFactory getBeanFactory(Class beanType) {
        AbstractBeanFactory beanFactory = null;
        if (beanFactoryMap.size() > 0) {
            for (Class type = beanType; type != null; type = type.getSuperclass()) {
                beanFactory = beanFactoryMap.get(type);
                if (beanFactory != null) {
                    break;
                }
            }
        }
        if (beanFactory == null) {
            beanFactory = defaultBeanFactory;
        }
        return beanFactory;
    }

    @Override
    public String toString() {
        return scanner.getRootPackages() + " @ size = " + beanDefinitionMap.size();
    }

    private void shutdownHook() {
        for (Map.Entry<String, BeanDefinition> entry : beanDefinitionMap.entrySet()) {
            String beanName = entry.getKey();
            BeanDefinition definition = entry.getValue();
            try {
                if (containsInstance(beanName) && isLifecycle(beanName)) {
                    Object bean = getBean(beanName, null, false);
                    if (bean == null) {
                        continue;
                    }
                    invokeBeanDestroy(beanName, bean, definition);
                }
            } catch (Exception e) {
                //skip
            }
        }
    }

    private void invokeBeanDestroy(String beanName, Object bean, BeanDefinition definition) throws IllegalStateException {
        boolean isDisposableBean = bean instanceof DisposableBean;
        if (isDisposableBean) {
            try {
                ((DisposableBean) bean).destroy();
            } catch (Exception e) {
                throw new IllegalStateException("invokeBeanDestroy destroy beanName=" + beanName + ".error=" + e, e);
            }
        }
        String destroyMethodName = definition.getDestroyMethodName();
        if (destroyMethodName != null && destroyMethodName.length() > 0 &&
                !(isDisposableBean && "destroy".equals(destroyMethodName))) {
            Class<?> beanClass = definition.getBeanClassIfResolve(resourceLoader);
            try {
                beanClass.getMethod(destroyMethodName).invoke(bean);
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw new IllegalStateException("invokeBeanDestroy destroyMethodName beanName=" + beanName + ",destroyMethodName" + destroyMethodName + ",error=" + e, e);
            }
        }
    }

    private void invokeBeanInitialization(String beanName, Object bean, BeanDefinition definition) throws IllegalStateException {
        boolean isInitializingBean = bean instanceof InitializingBean;
        if (isInitializingBean) {
            try {
                ((InitializingBean) bean).afterPropertiesSet();
            } catch (Exception e) {
                throw new IllegalStateException("invokeBeanInitialization afterPropertiesSet beanName=" + beanName + ".error=" + e, e);
            }
        }
        String initMethodName = definition.getInitMethodName();
        if (initMethodName != null && initMethodName.length() > 0 &&
                !(isInitializingBean && "afterPropertiesSet".equals(initMethodName))) {
            Class<?> beanClass = definition.getBeanClassIfResolve(resourceLoader);
            try {
                beanClass.getMethod(initMethodName).invoke(bean);
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw new IllegalStateException("invokeBeanInitialization initMethodName beanName=" + beanName + ",initMethodName" + initMethodName + ",error=" + e, e);
            }
        }
    }

    public String[] getBeanNames() {
        return beanDefinitionMap.keySet().toArray(new String[0]);
    }

    public Collection<Class<? extends Annotation>> getInitMethodAnnotations() {
        return initMethodAnnotations;
    }

    public Collection<Class<? extends Annotation>> getScannerAnnotations() {
        return scannerAnnotations;
    }

    public Collection<Class<? extends Annotation>> getAutowiredAnnotations() {
        return autowiredAnnotations;
    }

    public Collection<Class<? extends Annotation>> getQualifierAnnotations() {
        return qualifierAnnotations;
    }

    public Collection<Class<? extends Annotation>> getDestroyMethodAnnotations() {
        return destroyMethodAnnotations;
    }

    public Collection<Class<? extends Annotation>> getOrderedAnnotations() {
        return orderedAnnotations;
    }

    public Collection<Class<? extends Annotation>> getFactoryMethodAnnotations() {
        return factoryMethodAnnotations;
    }

    public Collection<BeanPostProcessor> getBeanPostProcessors() {
        return beanPostProcessors;
    }

    public Collection<String> getBeanSkipLifecycles() {
        return beanSkipLifecycles;
    }

    public Function<BeanDefinition, String> getBeanNameGenerator() {
        return beanNameGenerator;
    }

    public void setBeanNameGenerator(Function<BeanDefinition, String> beanNameGenerator) {
        this.beanNameGenerator = beanNameGenerator;
    }

    public boolean isLifecycle(String beanName) {
        return !beanSkipLifecycles.contains(beanName);
    }

    public Supplier<ClassLoader> getResourceLoader() {
        return resourceLoader;
    }

    public void setResourceLoader(Supplier<ClassLoader> resourceLoader) {
        this.resourceLoader = Objects.requireNonNull(resourceLoader);
    }

    public Collection<String> getRootPackageList() {
        return scanner.getRootPackages();
    }

    public interface AbstractBeanFactory {
        Object createBean(String beanName, BeanDefinition definition, Object[] args) throws RuntimeException;
    }

    public interface Aware {
    }

    public interface BeanNameAware extends Aware {
        void setBeanName(String name);
    }

    public interface ApplicationAware extends Aware {
        void setApplication(ApplicationContext applicationX);
    }

    public interface InitializingBean {
        void afterPropertiesSet() throws Exception;
    }

    public interface DisposableBean {
        void destroy() throws Exception;
    }

    public interface BeanPostProcessor {
        default Object postProcessBeforeInitialization(Object bean, String beanName) throws RuntimeException {
            return bean;
        }

        default Object postProcessAfterInitialization(Object bean, String beanName) throws RuntimeException {
            return bean;
        }
    }

    public interface MergedBeanDefinitionPostProcessor extends BeanPostProcessor {
        default void postProcessMergedBeanDefinition(BeanDefinition beanDefinition, Class<?> beanType, String beanName) {
        }

        default void resetBeanDefinition(String beanName) {
        }
    }

    public interface SmartInstantiationAwareBeanPostProcessor extends InstantiationAwareBeanPostProcessor {
        default Class<?> predictBeanType(Class<?> beanClass, String beanName) throws RuntimeException {
            return null;
        }

        default Constructor<?>[] determineCandidateConstructors(Class<?> beanClass, String beanName)
                throws RuntimeException {
            return null;
        }
    }

    public interface InstantiationAwareBeanPostProcessor extends BeanPostProcessor {
        default Object postProcessBeforeInstantiation(Class<?> beanClass, String beanName) throws RuntimeException {
            return null;
        }

        default boolean postProcessAfterInstantiation(Object bean, String beanName) throws RuntimeException {
            return true;
        }

        default PropertyValues postProcessProperties(PropertyValues pvs, Object bean, String beanName) throws RuntimeException {
            return pvs;
        }

        default PropertyValues postProcessPropertyValues(
                PropertyValues pvs, PropertyDescriptor[] pds, Object bean, String beanName) throws RuntimeException {
            return pvs;
        }
    }

    public interface ConversionService {
        default boolean canConvert(Class<?> sourceType, Class<?> targetType) {
            return true;
        }

        default <T> T convert(Object source, Class<T> targetType) {
            return (T) source;
        }
    }

    public interface PropertyEditor {
        /**
         * Gets the property value.
         *
         * @return The value of the property.  Primitive types such as "int" will
         * be wrapped as the corresponding object type such as "java.lang.Integer".
         */
        Object getValue();

        /**
         * Set (or change) the object that is to be edited.  Primitive types such
         * as "int" must be wrapped as the corresponding object type such as
         * "java.lang.Integer".
         *
         * @param value The new target object to be edited.  Note that this
         *              object should not be modified by the PropertyEditor, rather
         *              the PropertyEditor should create a new object to hold any
         *              modified value.
         */
        void setValue(Object value);
    }

    @Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    public @interface Bean {
        /**
         * The name of this bean, or if several names, a primary bean name plus aliases.
         * <p>If left unspecified, the name of the bean is the name of the annotated method.
         * If specified, the method name is ignored.
         *
         * @return String[]
         */
        String[] value() default {};
    }

    @Target(TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    public @interface Component {
        String value() default "";
    }

    @Target({CONSTRUCTOR, METHOD, PARAMETER, FIELD, ANNOTATION_TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    public @interface Autowired {
        boolean required() default true;
    }

    // TODO: 1月27日 027 @Value not impl config
    @Target({CONSTRUCTOR, METHOD, PARAMETER, FIELD, ANNOTATION_TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    public @interface Value {
        String value() default "";
    }

    @Target({TYPE, METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    public @interface Primary {
    }

    @Target({CONSTRUCTOR, METHOD, PARAMETER, FIELD, ANNOTATION_TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Resource {
        String name() default "";

        Class<?> type() default Object.class;
    }

    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(METHOD)
    public @interface PostConstruct {
    }

    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(METHOD)
    public @interface PreDestroy {
    }

    @Target({TYPE, METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Scope {
        String value() default BeanDefinition.SCOPE_SINGLETON;
    }

    @Target({TYPE, METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Lazy {
        boolean value() default true;
    }

    @Target({FIELD, METHOD, PARAMETER, TYPE, ANNOTATION_TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @Inherited
    @Documented
    public @interface Qualifier {
        String value() default "";
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD})
    @Documented
    public @interface Order {
        /**
         * 从小到大排列
         *
         * @return 排序
         */
        int value() default Integer.MAX_VALUE;
    }

    public interface Ordered {
        /**
         * 从小到大排列
         *
         * @return 排序
         */
        int getOrder();
    }

    /**
     * 1.扫描class文件
     * 2.创建对象并包装
     */
    public static class Scanner {
        private final Collection<String> rootPackages = new ArrayList<>(6);
        private final Collection<String> excludes = new LinkedHashSet<>(6);

        public Collection<String> getRootPackages() {
            return rootPackages;
        }

        public Collection<String> getExcludes() {
            return this.excludes;
        }

        public void doScan(String basePackage, String currentPackage, URL url, BiConsumer<URL, String> classConsumer) throws IOException {
            StringBuilder buffer = new StringBuilder();
            String splashPath = dotToSplash(basePackage);
            if (url == null || existContains(url)) {
                return;
            }
            String filePath = getRootPath(URLDecoder.decode(url.getFile(), "UTF-8"));
            List<String> names;
            if (isJarFile(filePath)) {
                names = readFromJarFile(filePath, splashPath);
            } else {
                names = readFromDirectory(filePath);
            }
            for (String name : names) {
                if (isClassFile(name)) {
                    String className = toClassName(buffer, name, currentPackage);
                    classConsumer.accept(url, className);
                } else {
                    String nextPackage;
                    if (currentPackage == null || currentPackage.isEmpty()) {
                        nextPackage = name;
                    } else {
                        nextPackage = currentPackage + "." + name;
                    }
                    doScan(basePackage, nextPackage, new URL(url + "/" + name), classConsumer);
                }
            }
        }

        public void doScan(String basePackage, ClassLoader loader, BiConsumer<URL, String> classConsumer) throws IOException {
            StringBuilder buffer = new StringBuilder();
            String splashPath = dotToSplash(basePackage);
            URL url = loader.getResource(splashPath);
            if (url == null || existContains(url)) {
                return;
            }
            String filePath = getRootPath(url.getFile());
            List<String> names;
            if (isJarFile(filePath)) {
                names = readFromJarFile(filePath, splashPath);
            } else {
                names = readFromDirectory(filePath);
            }
            for (String name : names) {
                if (isClassFile(name)) {
                    String className = toClassName(buffer, name, basePackage);
                    classConsumer.accept(url, className);
                } else {
                    doScan(basePackage + "." + name, loader, classConsumer);
                }
            }
        }

        private boolean existContains(URL url) {
            if (excludes.isEmpty()) {
                return false;
            }
            String[] urlStr = url.getPath().split("/");
            for (String s : excludes) {
                for (String u : urlStr) {
                    if (u.equals(s)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private String toClassName(StringBuilder buffer, String shortName, String basePackage) {
            buffer.setLength(0);
            shortName = trimExtension(shortName);
            if (shortName.contains(basePackage)) {
                buffer.append(shortName);
            } else {
                buffer.append(basePackage).append('.').append(shortName);
            }
            return buffer.toString();
        }

        //        if(jarPath.equals("/git/api/erp.jar"))
//        jarPath = "git/api/erp.jar";
        private List<String> readFromJarFile(String jarPath, String splashedPackageName) throws IOException {
            JarInputStream jarIn = new JarInputStream(new FileInputStream(jarPath));
            JarEntry entry = jarIn.getNextJarEntry();

            List<String> nameList = new ArrayList<>();
            while (null != entry) {
                String name = entry.getName();
                if (name.startsWith(splashedPackageName) && isClassFile(name)) {
                    nameList.add(name);
                }
                entry = jarIn.getNextJarEntry();
            }
            return nameList;
        }

        private List<String> readFromDirectory(String path) {
            File file = new File(path);
            String[] names = file.list();
            if (null == names) {
                return Collections.emptyList();
            }
            return Arrays.asList(names);
        }

        private boolean isClassFile(String name) {
            return name.endsWith(".class");
        }

        private boolean isJarFile(String name) {
            return name.endsWith(".jar");
        }

        private String getRootPath(String fileUrl) {
            int pos = fileUrl.indexOf('!');
            if (-1 == pos) {
                return fileUrl;
            }
            return fileUrl.substring(5, pos);
        }

        /**
         * "cn.fh.lightning" -> "cn/fh/lightning"
         */
        private String dotToSplash(String name) {
            return name.replaceAll("\\.", "/");
        }

        /**
         * "com/git/Apple.class" -> "com.git.Apple"
         */
        private String trimExtension(String name) {
            int pos = name.indexOf('.');
            if (-1 != pos) {
                name = name.substring(0, pos);
            }
            return name.replace("/", ".");
        }

        /**
         * /application/home -> /home
         */
        private String trimURI(String uri) {
            String trimmed = uri.substring(1);
            int splashIndex = trimmed.indexOf('/');
            return trimmed.substring(splashIndex);
        }

        @Override
        public String toString() {
            return "Scanner{" +
                    "rootPackages=" + rootPackages +
                    ", excludes=" + excludes +
                    '}';
        }
    }

    /**
     * 参考 org.springframework.beans.factory.annotation.InjectionMetadata.InjectedElement
     *
     * @param <T> 成员
     */
    public static class InjectElement<T extends Member> {
        private static final String[] QUALIFIER_FIELDS = new String[]{"value", "name"};
        /**
         * 成员有[构造器,方法,字段] {@link Constructor,Method,Field}
         */
        private final T member;
        /**
         * 用于获取注入参数
         */
        private final ApplicationContext applicationX;
        /**
         * 自动注入标记的注解
         */
        private final Annotation autowiredAnnotation;
        /**
         * 注入类型如下 {@link BeanDefinition#AUTOWIRE_BY_NAME,BeanDefinition#AUTOWIRE_BY_TYPE}
         */
        private final int[] autowireType;
        /**
         * 这个参数是否是必须的 (会覆盖这些参数是否是必须的)
         */
        private final Boolean[] requireds;
        /**
         * 这个包含泛型注入的类型
         */
        private Type[] requiredType;
        /**
         * 这个如果是泛型注入,则Class会是Object, 所以需要requiredType
         */
        private Class[] requiredClass;
        /**
         * 如果根据名称注入
         */
        private String[] requiredName;
        /**
         * 这些参数是否是必须的
         */
        private Boolean required;

        public InjectElement(Executable executable, ApplicationContext applicationX) {
            int parameterCount = executable.getParameterCount();
            this.member = (T) executable;
            this.applicationX = applicationX;
            this.autowiredAnnotation = findDeclaredAnnotation(executable, applicationX.autowiredAnnotations, AUTOWIRED_ANNOTATION_CACHE_MAP);
            this.autowireType = new int[parameterCount];
            this.requiredClass = new Class[parameterCount];
            this.requiredType = new Type[parameterCount];
            this.requiredName = new String[parameterCount];
            this.requireds = new Boolean[parameterCount];

            Parameter[] parameters = executable.getParameters();
            for (int i = 0; i < parameterCount; i++) {
                Parameter parameter = parameters[i];
                this.requiredClass[i] = parameter.getType();
                this.autowireType[i] = applicationX.findAutowireType(parameter);
                switch (this.autowireType[i]) {
                    case BeanDefinition.AUTOWIRE_BY_TYPE: {
                        Annotation parameterInjectAnnotation = findDeclaredAnnotation(parameter, applicationX.autowiredAnnotations, AUTOWIRED_ANNOTATION_CACHE_MAP);
                        this.requiredType[i] = findAnnotationDeclaredType(parameterInjectAnnotation, parameter.getParameterizedType());
                        break;
                    }
                    case BeanDefinition.AUTOWIRE_BY_NAME: {
                        Annotation qualifierAnnotation = findDeclaredAnnotation(parameter, applicationX.qualifierAnnotations, QUALIFIER_ANNOTATION_CACHE_MAP);
                        String autowiredBeanName = qualifierAnnotation != null ?
                                getQualifierAnnotationValue(qualifierAnnotation) : parameter.getName();
                        this.requiredName[i] = autowiredBeanName;
                        break;
                    }
                    default: {
                        break;
                    }
                }
                Annotation parameterAutowiredAnnotation = findDeclaredAnnotation(parameter, applicationX.autowiredAnnotations, AUTOWIRED_ANNOTATION_CACHE_MAP);
                this.requireds[i] = parameterAutowiredAnnotation != null ?
                        getAnnotationValue(parameterAutowiredAnnotation, "required", Boolean.class) : null;
            }
            if (this.autowiredAnnotation != null) {
                this.required = getAnnotationValue(this.autowiredAnnotation, "required", Boolean.class);
            }
        }

        public InjectElement(Field field, ApplicationContext applicationX) {
            this.member = (T) field;
            this.applicationX = applicationX;
            this.autowiredAnnotation = findDeclaredAnnotation(field, applicationX.autowiredAnnotations, AUTOWIRED_ANNOTATION_CACHE_MAP);
            this.autowireType = new int[]{applicationX.findAutowireType(field)};
            this.requiredClass = new Class[]{field.getType()};
            switch (this.autowireType[0]) {
                case BeanDefinition.AUTOWIRE_BY_TYPE: {
                    this.requiredType = new Type[]{findAnnotationDeclaredType(this.autowiredAnnotation, field.getGenericType())};
                    break;
                }
                case BeanDefinition.AUTOWIRE_BY_NAME: {
                    Annotation qualifierAnnotation = findDeclaredAnnotation(field, applicationX.qualifierAnnotations, QUALIFIER_ANNOTATION_CACHE_MAP);
                    String autowiredBeanName = qualifierAnnotation != null ?
                            getQualifierAnnotationValue(qualifierAnnotation) : field.getName();
                    this.requiredName = new String[]{autowiredBeanName};
                    break;
                }
                default: {
                    break;
                }
            }
            if (this.autowiredAnnotation != null) {
                this.required = getAnnotationValue(this.autowiredAnnotation, "required", Boolean.class);
            }
            this.requireds = new Boolean[]{this.required};
        }

        private static String getQualifierAnnotationValue(Annotation qualifierAnnotation) {
            return getAnnotationValue(qualifierAnnotation, QUALIFIER_FIELDS, String.class);
        }

        public static List<InjectElement<Field>> getInjectFields(Class rootClass, ApplicationContext applicationX) {
            List<InjectElement<Field>> list = new ArrayList<>();
            for (Class clazz = rootClass; clazz != null && clazz != Object.class; clazz = clazz.getSuperclass()) {
                for (Field field : clazz.getDeclaredFields()) {
                    //寻找打着注解的字段
                    if (null != findDeclaredAnnotation(field, applicationX.autowiredAnnotations, AUTOWIRED_ANNOTATION_CACHE_MAP)) {
                        InjectElement<Field> element = new InjectElement<>(field, applicationX);
                        list.add(element);
                    }
                }
            }
            return list;
        }

        public static List<InjectElement<Method>> getInjectMethods(Class rootClass, ApplicationContext applicationX) {
            List<InjectElement<Method>> result = new ArrayList<>();
            eachClass(rootClass, clazz -> {
                for (Method method : getDeclaredMethods(clazz)) {
                    //寻找打着注解的方法
                    if (null != findDeclaredAnnotation(method, applicationX.autowiredAnnotations, AUTOWIRED_ANNOTATION_CACHE_MAP)) {
                        result.add(new InjectElement<>(method, applicationX));
                    }
                }
            });
            return result;
        }

        private static Class findConcreteClass(Class<?> parameterGenericClass, Class concreteChildClass) {
            BiFunction<Type, Class<?>, Class<?>> findFunction = (generic, genericSuper) -> {
                if (generic instanceof ParameterizedType) {
                    for (Type actualTypeArgument : ((ParameterizedType) generic).getActualTypeArguments()) {
                        if (actualTypeArgument instanceof Class
                                && genericSuper.isAssignableFrom((Class<?>) actualTypeArgument)) {
                            return (Class) actualTypeArgument;
                        }
                    }
                }
                return null;
            };
            Class<?> result = findFunction.apply(concreteChildClass.getGenericSuperclass(), parameterGenericClass);
            if (result == null) {
                for (Type genericInterface : concreteChildClass.getGenericInterfaces()) {
                    if (null != (result = findFunction.apply(genericInterface, parameterGenericClass))) {
                        break;
                    }
                }
            }
            return result == null ? parameterGenericClass : result;
        }

        private static Type findAnnotationDeclaredType(Annotation annotation, Type def) {
            if (annotation == null) {
                return def;
            }
            Type annotationDeclaredType = getAnnotationValue(annotation, "type", Type.class);
            if (annotationDeclaredType != null && annotationDeclaredType != Object.class) {
                return annotationDeclaredType;
            } else {
                return def;
            }
        }

        /**
         * 会根据类型或名称调用getBean()方法, 返回需要的所有参数. {@link BeanDefinition#AUTOWIRE_BY_TYPE,BeanDefinition#AUTOWIRE_BY_NAME}
         *
         * @param targetClass 注入目标类
         * @return 从容器中取出的多个bean
         * @throws IllegalStateException 如果容器中不存在需要的bean
         */
        private Object[] getInjectValues(Class targetClass) throws IllegalStateException {
            Boolean defaultRequired = this.required;
            if (defaultRequired == null) {
                defaultRequired = Boolean.FALSE;
            }

            Object[] values = new Object[autowireType.length];
            for (int i = 0; i < autowireType.length; i++) {
                Object injectResource;
                Boolean required = requireds[i];
                if (required == null) {
                    required = defaultRequired;
                }
                Object desc;
                switch (autowireType[i]) {
                    case BeanDefinition.AUTOWIRE_BY_NAME: {
                        desc = requiredName[i];
                        injectResource = applicationX.getBean(requiredName[i], null, false);
                        break;
                    }
                    case BeanDefinition.AUTOWIRE_BY_TYPE:
                    default: {
                        Class<?> autowireClass = requiredType[i] instanceof Class ?
                                (Class) requiredType[i] : findConcreteClass(requiredClass[i], targetClass);
                        desc = autowireClass;
                        if (autowireClass == Object.class) {
                            injectResource = null;
                        } else if (isAbstract(autowireClass)) {
                            List implList = applicationX.getBeanForType(autowireClass);
                            injectResource = implList.isEmpty() ? null : implList.get(0);
                        } else {
                            injectResource = applicationX.getBean(autowireClass, null, false);
                        }
                        break;
                    }
                }
                if (injectResource == null && required) {
                    throw new IllegalStateException("Required part[" + (i + 1) + "] '" + desc + "' is not present. member='" + member + "',class=" + member.getDeclaringClass() + ". Dependency annotations: Autowired(required=false)");
                }
                values[i] = injectResource;
            }
            return values;
        }

        /**
         * 注入
         *
         * @param target      需要注入的实例
         * @param targetClass 需要注入的原始类型,用于查找泛型
         * @return 如果是方法, 则返回方法返回值. 如果是构造器,返回实例. 如果是字段返回null
         * @throws IllegalStateException 注入异常
         */
        public Object inject(Object target, Class targetClass) throws IllegalStateException {
            if (targetClass == null) {
                targetClass = target.getClass();
            }
            if (this.member instanceof Field) {
                Field field = (Field) this.member;
                if (Modifier.isFinal(field.getModifiers())) {
                    return null;
                }
                //获取注入的参数
                Object[] values = getInjectValues(targetClass);
                try {
                    boolean accessible = field.isAccessible();
                    try {
                        //调用java的字段赋值, 相当于this.field = value
                        field.setAccessible(true);
                        field.set(target, values[0]);
                    } finally {
                        field.setAccessible(accessible);
                    }
                } catch (Throwable e) {
                    throw new IllegalStateException("inject error=" + e + ". class=" + target.getClass() + ",field=" + this.member);
                }
            } else if (this.member instanceof Method) {
                Method method = (Method) this.member;
                Object[] values = getInjectValues(targetClass);
                try {
                    boolean accessible = method.isAccessible();
                    try {
                        //调用java的字段赋值, 相当于setValue(values)
                        method.setAccessible(true);
                        return method.invoke(target, values);
                    } finally {
                        method.setAccessible(accessible);
                    }
                } catch (Throwable e) {
                    throw new IllegalStateException("inject error=" + e + ". class=" + target.getClass() + ",method=" + this.member);
                }
            } else if (this.member instanceof Constructor) {
                return newInstance(null);
            }
            return null;
        }

        public Object newInstance(Object[] args) throws IllegalStateException {
            //不能创建枚举类
            if (this.member.getDeclaringClass().isEnum()) {
                return null;
            }
            if (!(this.member instanceof Constructor)) {
                throw new IllegalStateException("member not instanceof Constructor!");
            }
            Constructor constructor = (Constructor) this.member;
            //如果用户在getBean(name,args)没有传参数
            if (args == null || args.length == 0) {
                //获取注入的参数
                args = getInjectValues(member.getDeclaringClass());
            }
            boolean accessible = constructor.isAccessible();
            try {
                //相当于 new MyBean(args)
                constructor.setAccessible(true);
                Object instance = constructor.newInstance(args);
                return instance;
            } catch (IllegalAccessException | InstantiationException |
                    InvocationTargetException | IllegalArgumentException |
                    ExceptionInInitializerError e) {
                throw new IllegalStateException("inject error=" + e + ". method=" + this.member, e);
            } finally {
                constructor.setAccessible(accessible);
            }
        }
    }

    private static class DefaultBeanNameGenerator implements Function<BeanDefinition, String> {
        private final Map<Class, Boolean> scannerAnnotationCacheMap = newConcurrentReferenceMap(32);
        private final ApplicationContext applicationX;

        public DefaultBeanNameGenerator(ApplicationContext applicationX) {
            this.applicationX = Objects.requireNonNull(applicationX);
        }

        @Override
        public String apply(BeanDefinition definition) {
            Class beanClass = definition.getBeanClassIfResolve(applicationX.resourceLoader);
            Annotation annotation = findDeclaredAnnotation(beanClass, applicationX.scannerAnnotations, scannerAnnotationCacheMap);
            String beanName = null;
            if (annotation != null) {
                beanName = getAnnotationValue(annotation, "value", String.class);
            }
            if (beanName == null || beanName.isEmpty()) {
                String className = beanClass.getName();
                int lastDotIndex = className.lastIndexOf('.');
                int nameEndIndex = className.indexOf("$$");
                if (nameEndIndex == -1) {
                    nameEndIndex = className.length();
                }
                String shortName = className.substring(lastDotIndex + 1, nameEndIndex);
                shortName = shortName.replace('$', '.');
                beanName = Introspector.decapitalize(shortName);
            }
            return beanName;
        }
    }

    public static class BeanDefinition {
        public static final String SCOPE_SINGLETON = "singleton";
        public static final String SCOPE_PROTOTYPE = "prototype";
        public static final int AUTOWIRE_NO = 0;
        public static final int AUTOWIRE_BY_NAME = 1;
        public static final int AUTOWIRE_BY_TYPE = 2;
        public static final int AUTOWIRE_CONSTRUCTOR = 3;
        public static final int DEPENDENCY_CHECK_NONE = 0;
        public static final int DEPENDENCY_CHECK_OBJECTS = 1;
        public static final int DEPENDENCY_CHECK_SIMPLE = 2;
        public static final int DEPENDENCY_CHECK_ALL = 3;
        final Object postProcessingLock = new Object();
        private final Map<String, Object> attributes = new LinkedHashMap<>();
        private final Map<Integer, ValueHolder> constructorArgumentValues = new LinkedHashMap<>();
        private boolean postProcessed = false;
        private int dependencyCheck = DEPENDENCY_CHECK_NONE;
        private Supplier<?> beanSupplier;
        private Object beanClass;
        private String beanClassName;
        private String scope = SCOPE_SINGLETON;
        private boolean primary = false;
        private boolean lazyInit = false;
        private String initMethodName;
        private String destroyMethodName;
        private int autowireMode = AUTOWIRE_NO;
        private PropertyValues propertyValues = PropertyValues.EMPTY;
        private boolean allowCaching = true;
        //用于aop等代理对象
        private volatile Boolean beforeInstantiationResolved;

        public BeanDefinition() {
        }

        public Map<Integer, ValueHolder> getConstructorArgumentValues() {
            return constructorArgumentValues;
        }

        public String getDestroyMethodName() {
            return destroyMethodName;
        }

        public void setDestroyMethodName(String destroyMethodName) {
            this.destroyMethodName = destroyMethodName;
        }

        public int getDependencyCheck() {
            return dependencyCheck;
        }

        public void setDependencyCheck(int dependencyCheck) {
            this.dependencyCheck = dependencyCheck;
        }

        public String getInitMethodName() {
            return initMethodName;
        }

        public void setInitMethodName(String initMethodName) {
            this.initMethodName = initMethodName;
        }

        public boolean isSingleton() {
            return SCOPE_SINGLETON.equals(scope);
        }

        public boolean isPrototype() {
            return SCOPE_PROTOTYPE.equals(scope);
        }

        public boolean isLazyInit() {
            return lazyInit;
        }

        public void setLazyInit(boolean lazyInit) {
            this.lazyInit = lazyInit;
        }

        public boolean isPrimary() {
            return primary;
        }

        public void setPrimary(boolean primary) {
            this.primary = primary;
        }

        public String getScope() {
            return scope;
        }

        public void setScope(String scope) {
            this.scope = scope;
        }

        public PropertyValues getPropertyValues() {
            return this.propertyValues;
        }

        public void setPropertyValues(PropertyValues propertyValues) {
            this.propertyValues = propertyValues;
        }

        public Boolean getBeforeInstantiationResolved() {
            return beforeInstantiationResolved;
        }

        public void setBeforeInstantiationResolved(Boolean beforeInstantiationResolved) {
            this.beforeInstantiationResolved = beforeInstantiationResolved;
        }

        public Class getBeanClass() {
            if (beanClass == null) {
                throw new IllegalStateException("No bean class specified on bean definition");
            }
            if (!(beanClass instanceof Class)) {
                throw new IllegalStateException(
                        "Bean class name [" + beanClass + "] has not been resolved into an actual Class");
            }
            return (Class) beanClass;
        }

        public void setBeanClass(Class beanClass) {
            this.beanClass = beanClass;
        }

        public Class getBeanClassIfResolve(Supplier<ClassLoader> loaderSupplier) {
            if (beanClass == null || !(beanClass instanceof Class)) {
                beanClass = resolveBeanClass(loaderSupplier.get());
            }
            return (Class) beanClass;
        }

        public Class resolveBeanClass(ClassLoader classLoader) {
            try {
                return Class.forName(beanClassName, false, classLoader);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("getBeanClass error." + e, e);
            }
        }

        public Supplier<?> getBeanSupplier() {
            return beanSupplier;
        }

        public void setBeanSupplier(Supplier<?> beanSupplier) {
            this.beanSupplier = beanSupplier;
        }

        public void setAttribute(String name, Object value) {
            attributes.put(name, value);
        }

        public Object removeAttribute(String name) {
            return attributes.remove(name);
        }

        public Object getAttribute(String name) {
            return attributes.get(name);
        }

        public String getBeanClassName() {
            return beanClassName;
        }

        public void setBeanClassName(String beanClassName) {
            this.beanClassName = beanClassName;
        }

        public int getAutowireMode() {
            return this.autowireMode;
        }

        public void setAutowireMode(int autowireMode) {
            this.autowireMode = autowireMode;
        }

        @Override
        public String toString() {
            return scope + '{' + beanClassName + '}';
        }
    }

    public static class ValueHolder {
        private Object value;
        private String type;
        private String name;
        private Object source;
        private boolean converted = false;
        private Object convertedValue;

        public ValueHolder(Object value) {
            this.value = value;
        }
    }

    public static class PropertyValues implements Iterable<PropertyValue> {
        public static PropertyValues EMPTY = new PropertyValues(new PropertyValue[0]);
        private PropertyValue[] propertyValues;

        public PropertyValues(PropertyValue[] propertyValues) {
            this.propertyValues = propertyValues;
        }

        @Override
        public Iterator<PropertyValue> iterator() {
            return Arrays.asList(getPropertyValues()).iterator();
        }

        @Override
        public Spliterator<PropertyValue> spliterator() {
            return Spliterators.spliterator(getPropertyValues(), 0);
        }

        public Stream<PropertyValue> stream() {
            return StreamSupport.stream(spliterator(), false);
        }

        public PropertyValue[] getPropertyValues() {
            return propertyValues;
        }

        public boolean contains(String propertyName) {
            for (PropertyValue value : propertyValues) {
                if (Objects.equals(propertyName, value.name)) {
                    return true;
                }
            }
            return false;
        }

        public boolean isEmpty() {
            return propertyValues.length == 0;
        }
    }

    /*==============static-utils=============================*/

    public static class PropertyValue {
        private final Map<String, Object> attributes = new LinkedHashMap<>();
        private final String name;
        private final Object value;
        private Object source;
        private boolean optional = false;
        private boolean converted = false;
        private Object convertedValue;

        public PropertyValue(String name, Object value) {
            this.name = name;
            this.value = value;
        }
    }

    public static class OrderComparator implements Comparator<Object> {
        private final Collection<Class<? extends Annotation>> orderedAnnotations;

        public OrderComparator(Collection<Class<? extends Annotation>> orderedAnnotations) {
            this.orderedAnnotations = Objects.requireNonNull(orderedAnnotations);
        }

        @Override
        public int compare(Object o1, Object o2) {
            int c1 = convertInt(o1);
            int c2 = convertInt(o2);
            return c1 < c2 ? -1 : 1;
        }

        protected int convertInt(Object o) {
            Annotation annotation;
            int order;
            if (o == null) {
                order = Integer.MAX_VALUE;
            } else if (o instanceof Ordered) {
                order = ((Ordered) o).getOrder();
            } else if ((annotation = findAnnotation(o.getClass(), orderedAnnotations)) != null) {
                Number value = getAnnotationValue(annotation, "value", Number.class);
                if (value != null) {
                    order = value.intValue();
                } else {
                    order = Integer.MAX_VALUE;
                }
            } else {
                order = Integer.MAX_VALUE;
            }
            return order;
        }
    }

    @Order(Integer.MIN_VALUE + 10)
    public static class RegisteredBeanPostProcessor implements BeanPostProcessor {
        private final ApplicationContext applicationX;

        public RegisteredBeanPostProcessor(ApplicationContext applicationX) {
            this.applicationX = Objects.requireNonNull(applicationX);
        }

        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) throws RuntimeException {
            if (bean instanceof BeanPostProcessor) {
                applicationX.addBeanPostProcessor((BeanPostProcessor) bean);
            }
            return bean;
        }
    }

    /**
     * 参考 org.springframework.beans.factory.annotation.AutowiredAnnotationBeanPostProcessor
     */
    @Order(Integer.MIN_VALUE + 20)
    public static class AutowiredConstructorPostProcessor implements SmartInstantiationAwareBeanPostProcessor, MergedBeanDefinitionPostProcessor {
        private static final Constructor[] EMPTY = {};
        private final ApplicationContext applicationX;
        //如果字段参数注入缺少参数, 是否抛出异常
        private boolean defaultInjectRequiredField = true;
        //如果方法参数注入缺少参数, 是否抛出异常
        private boolean defaultInjectRequiredMethod = true;

        public AutowiredConstructorPostProcessor(ApplicationContext applicationX) {
            this.applicationX = Objects.requireNonNull(applicationX);
        }

        @Override
        public void postProcessMergedBeanDefinition(BeanDefinition definition, Class<?> beanType, String beanName) {
            eachClass(beanType, clazz -> {
                for (Method method : getDeclaredMethods(clazz)) {
                    //寻找工厂bean, 例: 打着@Bean注解的. 也就是spring中的合成bean
                    Annotation factoryMethodAnnotation = findDeclaredAnnotation(method, applicationX.factoryMethodAnnotations, FACTORY_METHOD_ANNOTATION_CACHE_MAP);
                    if (factoryMethodAnnotation != null) {
                        addBeanDefinition(method, factoryMethodAnnotation, beanName, beanType);
                    }
                }
            });
        }

        @Override
        public Constructor<?>[] determineCandidateConstructors(Class<?> beanClass, String beanName) throws RuntimeException {
            //这里负责选出多个候选的构造器, 如果需要用无参构造,则需要返回null.
            List<Constructor<?>> list = new LinkedList<>();
            Constructor<?>[] constructors = beanClass.getDeclaredConstructors();
            for (Constructor<?> constructor : constructors) {
                //如果是合成构造器 (合成构造器是由编译器生成的构造,比如无参构造器)
                if (constructor.isSynthetic()) {
                    return null;
                }
                //如果是无参构造器
                if (constructor.getParameterCount() == 0 && Modifier.isPublic(constructor.getModifiers())) {
                    return null;
                }
            }
            for (Constructor<?> constructor : constructors) {
                if (Modifier.isPublic(constructor.getModifiers())) {
                    list.add(constructor);
                }
            }
            if (list.isEmpty()) {
                throw new IllegalStateException("No visible constructors in " + beanName);
            }
            return list.size() == constructors.length ? constructors : list.toArray(EMPTY);
        }

        @Override
        public boolean postProcessAfterInstantiation(Object bean, String beanName) throws RuntimeException {
            BeanDefinition definition = applicationX.getBeanDefinition(beanName);
            //获取用户定义的类型
            Class beanClass = definition.getBeanClassIfResolve(applicationX.getResourceLoader());
            if (isAbstract(beanClass)) {
                beanClass = bean.getClass();
            }
            inject(bean, beanClass);
            return true;
        }

        /**
         * 调用setter方法, 与字段赋值. 例如: this.myData = myData
         * 现在正处在bean刚实例化完的时候, 相当于刚new Bean()完的事件通知中.这时还是刚创建的bean, 很干净.要给它注入对象
         *
         * @param bean      刚创建的bean
         * @param beanClass 不是抽象的类型
         */
        private void inject(Object bean, Class beanClass) {
            //获取需要注入的字段, 比如打过注解(@Autowired)的字段
            List<InjectElement<Field>> declaredFields = InjectElement.getInjectFields(beanClass, applicationX);
            //获取需要注入的方法. 比如打过注解(@Autowired)的setter方法.
            List<InjectElement<Method>> declaredMethods = InjectElement.getInjectMethods(beanClass, applicationX);
            for (InjectElement<Field> element : declaredFields) {
                if (element.required == null) {
                    element.required = defaultInjectRequiredField;
                }
                element.inject(bean, beanClass);
            }
            for (InjectElement<Method> element : declaredMethods) {
                if (element.required == null) {
                    element.required = defaultInjectRequiredMethod;
                }
                element.inject(bean, beanClass);
            }
        }

        private void addBeanDefinition(Method method, Annotation factoryMethodAnnotation, String factoryBeanName, Class<?> factoryBeanClass) {
            String[] beanNames = getAnnotationValue(factoryMethodAnnotation, "value", String[].class);
            LinkedList<String> beanNameList = new LinkedList<>(beanNames == null || beanNames.length == 0 ?
                    Arrays.asList(method.getName()) : Arrays.asList(beanNames));
            String beanName = beanNameList.pollFirst();

            BeanDefinition definition = applicationX.newBeanDefinition(method.getReturnType(), method);
            InjectElement<Method> element = new InjectElement<>(method, applicationX);
            definition.setBeanSupplier(() -> {
                Object bean = element.applicationX.getBean(factoryBeanName);
                return element.inject(bean, factoryBeanClass);
            });
            applicationX.addBeanDefinition(beanName, definition);
            for (String alias : beanNameList) {
                applicationX.registerAlias(beanName, alias);
            }
        }
    }

    public static class BeanWrapper {
        /**
         * Path separator for nested properties.
         * Follows normal Java conventions: getFoo().getBar() would be "foo.bar".
         */
        public static final String NESTED_PROPERTY_SEPARATOR = ".";

        /**
         * Path separator for nested properties.
         * Follows normal Java conventions: getFoo().getBar() would be "foo.bar".
         */
        public static final char NESTED_PROPERTY_SEPARATOR_CHAR = '.';

        /**
         * Marker that indicates the start of a property key for an
         * indexed or mapped property like "person.addresses[0]".
         */
        public static final String PROPERTY_KEY_PREFIX = "[";

        /**
         * Marker that indicates the start of a property key for an
         * indexed or mapped property like "person.addresses[0]".
         */
        public static final char PROPERTY_KEY_PREFIX_CHAR = '[';

        /**
         * Marker that indicates the end of a property key for an
         * indexed or mapped property like "person.addresses[0]".
         */
        public static final String PROPERTY_KEY_SUFFIX = "]";

        /**
         * Marker that indicates the end of a property key for an
         * indexed or mapped property like "person.addresses[0]".
         */
        public static final char PROPERTY_KEY_SUFFIX_CHAR = ']';
        //by org.springframework.beans.ConfigurablePropertyAccessor
        private ConversionService conversionService;
        private Object wrappedInstance;
        private Class<?> wrappedClass;
        private PropertyDescriptor[] cachedIntrospectionResults;

        public BeanWrapper(Object wrappedInstance) {
            this.wrappedInstance = wrappedInstance;
            this.wrappedClass = wrappedInstance.getClass();
        }

        public PropertyDescriptor[] getPropertyDescriptors() {
            if (cachedIntrospectionResults == null) {
                cachedIntrospectionResults = getPropertyDescriptorsIfCache(wrappedClass);
            }
            return cachedIntrospectionResults;
        }

        public Class<?> getWrappedClass() {
            return wrappedClass;
        }

        public Object getWrappedInstance() {
            return wrappedInstance;
        }

        public boolean isReadableProperty(String propertyName) {
            PropertyDescriptor descriptor = getPropertyDescriptor(propertyName);
            if (descriptor == null) {
                return false;
            }
            return descriptor.getReadMethod() != null;
        }

        public boolean isWritableProperty(String propertyName) {
            PropertyDescriptor descriptor = getPropertyDescriptor(propertyName);
            if (descriptor == null) {
                return false;
            }
            return descriptor.getWriteMethod() != null;
        }

        public Class<?> getPropertyType(String propertyName) throws IllegalArgumentException, IllegalStateException {
            PropertyDescriptor descriptor = getPropertyDescriptor(propertyName);
            if (descriptor == null) {
                throw new IllegalArgumentException("No property handler found");
            }
            return descriptor.getPropertyType();
        }

        public Type getPropertyTypeDescriptor(String propertyName) throws IllegalArgumentException, IllegalStateException {
            return getPropertyType(propertyName);
        }

        public Object getPropertyValue(String propertyName) throws IllegalArgumentException, IllegalStateException {
            PropertyDescriptor descriptor = getPropertyDescriptor(propertyName);
            if (descriptor == null) {
                throw new IllegalArgumentException("No property handler found");
            }
            Method readMethod = descriptor.getReadMethod();
            if (readMethod == null) {
                throw new IllegalStateException("Not readable. name=" + propertyName);
            }
            try {
                return readMethod.invoke(wrappedInstance);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException("readMethod error. name=" + propertyName, e);
            }
        }

        public void setPropertyValue(String propertyName, Object value) throws IllegalArgumentException, IllegalStateException {
            PropertyDescriptor descriptor = getPropertyDescriptor(propertyName);
            if (descriptor == null) {
                throw new IllegalArgumentException("No property handler found");
            }
            Object convertedResult = convertIfNecessary(value, descriptor.getPropertyType());
            Method writeMethod = descriptor.getWriteMethod();
            if (writeMethod == null) {
                throw new IllegalStateException("Not writable. name=" + propertyName);
            }
            try {
                writeMethod.invoke(wrappedInstance, convertedResult);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException("writeMethod error. name=" + propertyName, e);
            }
        }

        public void setPropertyValue(PropertyValue pv) throws IllegalArgumentException, IllegalStateException {
            // TODO: 1月26日 026 setPropertyValue
            setPropertyValue(pv.name, pv.value);
        }

        public void setPropertyValues(Map<?, ?> map) throws IllegalArgumentException, IllegalStateException {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                setPropertyValue(entry.getKey().toString(), entry.getValue());
            }
        }

        public void setPropertyValues(PropertyValues pvs) throws IllegalArgumentException, IllegalStateException {
            setPropertyValues(pvs, false, false);
        }

        public void setPropertyValues(PropertyValues pvs, boolean ignoreUnknown)
                throws IllegalArgumentException, IllegalStateException {
            setPropertyValues(pvs, ignoreUnknown, false);
        }

        public void setPropertyValues(PropertyValues pvs, boolean ignoreUnknown, boolean ignoreInvalid)
                throws IllegalArgumentException, IllegalStateException {
            for (PropertyValue pv : pvs) {
                try {
                    setPropertyValue(pv);
                } catch (IllegalArgumentException e) {
                    if (!ignoreUnknown) {
                        throw e;
                    }
                } catch (IllegalStateException e) {
                    if (!ignoreInvalid) {
                        throw e;
                    }
                }
            }
        }

        public PropertyDescriptor getPropertyDescriptor(String propertyName) throws IllegalArgumentException {
            for (PropertyDescriptor descriptor : getPropertyDescriptors()) {
                if (descriptor.getName().equals(propertyName)) {
                    return descriptor;
                }
            }
            return null;
        }

        //by org.springframework.beans.TypeConverter
        public <T> T convertIfNecessary(Object value, Class<T> requiredType) throws IllegalArgumentException {
            Class<?> sourceType = value != null ? value.getClass() : null;
            Class<?> targetType = requiredType;
            Object convertValue = value;
            if (conversionService.canConvert(sourceType, targetType)) {
                convertValue = conversionService.convert(value, targetType);
            }
            return (T) convertValue;
        }
    }

    public class ScannerResult {
        private final List<ClassLoader> classLoaderList = new ArrayList<>();
        private final Set<URL> classUrls = new LinkedHashSet<>();
        private final Map<String, BeanDefinition> beanDefinitionMap = new ConcurrentHashMap<>(64);
        private final Map<Class, Boolean> scannerAnnotationCacheMap = new ConcurrentHashMap<>(64);
        public AtomicInteger classCount = new AtomicInteger();

        public int inject() {
            LinkedList<String> beanNameList = new LinkedList<>();
            for (Map.Entry<String, BeanDefinition> entry : beanDefinitionMap.entrySet()) {
                String beanName = entry.getKey();
                BeanDefinition definition = entry.getValue();

                addBeanDefinition(beanName, definition);
                if (definition.isSingleton() && !definition.isLazyInit()) {
                    if (BeanPostProcessor.class.isAssignableFrom(definition.getBeanClass())) {
                        beanNameList.addFirst(beanName);
                    } else {
                        beanNameList.addLast(beanName);
                    }
                }
            }
            for (String beanName : beanNameList) {
                getBean(beanName, null, true);
            }
            beanDefinitionMap.clear();
            scannerAnnotationCacheMap.clear();
            return beanNameList.size();
        }
    }

    private class DefaultBeanFactory implements AbstractBeanFactory {
        /**
         * 缓存检查环依赖的属性
         */
        private final Map<Class<?>, PropertyDescriptor[]> filteredPropertyDescriptorsCache = new ConcurrentHashMap<>();
        //如果构造参数注入缺少参数, 是否抛出异常
        private boolean defaultInjectRequiredConstructor = true;

        @Override
        public Object createBean(String beanName, BeanDefinition definition, Object[] args) {
            //如果不等于空, 说明在事件通知时, 用户返回了实例,不需要创建. 这样会不受下面的生命周期控制 (通常用于代理)
            Object bean = resolveBeforeInstantiation(beanName, definition);
            if (bean != null) {
                return bean;
            }
            // TODO: 1月29日 029  createBean Prepare method overrides.
            return doCreateBean(beanName, definition, args);
        }

        protected Object doCreateBean(String beanName, BeanDefinition definition, Object[] args) {
            //创建实例, 等同于newInstance(); 这时只是一个空的实例.
            BeanWrapper beanInstanceWrapper = createBeanInstance(beanName, definition, args);
            //最终暴露给用户的实例
            Object exposedObject = beanInstanceWrapper.getWrappedInstance();
            Class<?> beanType = beanInstanceWrapper.getWrappedClass();
            //一个BeanDefinition只会通知一次合成bean事件
            synchronized (definition.postProcessingLock) {
                if (!definition.postProcessed) {
                    try {
                        //通知定义合并bean, 合成bean是一个bean里，会创建多个子孙bean. 例如@Bean注解的实现
                        applyMergedBeanDefinitionPostProcessors(definition, beanType, beanName);
                    } catch (Throwable ex) {
                        throw new IllegalStateException("Post-processing of merged bean definition failed. beanName=" + beanName, ex);
                    }
                    definition.postProcessed = true;
                }
            }

            //如果需要生命周期管理 (这个方法在spring里没有,这里为了满足我的特殊需求用的, 我需要对某些bean强制跳过生命周期管理)
            if (isLifecycle(beanName)) {
                //填充bean属性, 也就是自动注入.与PostProcessor事件
                populateBean(beanName, definition, beanInstanceWrapper);
                //执行我们自己定义的初始化方法, 与执行bean的生命周期方法与PostProcessor事件,
                //例如: Aware接口, @PostConstruct,InitializingBean.afterPropertiesSet();
                exposedObject = initializeBean(beanName, beanInstanceWrapper, definition);
            }
            return exposedObject;
        }

        /**
         * 通知合成bean的配置
         *
         * @param mbd      在spring里是MultiBeanDefinition的意思(多个BeanDefinition),
         *                 spring的BeanDefinition里有个parent字段.
         *                 我这里简化了实现, 只有一个.
         * @param beanType bean的原始类型(可能层层包装后,类型在不断的变化)
         * @param beanName bean的名称
         */
        protected void applyMergedBeanDefinitionPostProcessors(BeanDefinition mbd, Class<?> beanType, String beanName) {
            for (BeanPostProcessor bp : new ArrayList<>(beanPostProcessors)) {
                if (bp instanceof MergedBeanDefinitionPostProcessor) {
                    MergedBeanDefinitionPostProcessor bdp = (MergedBeanDefinitionPostProcessor) bp;
                    bdp.postProcessMergedBeanDefinition(mbd, beanType, beanName);
                }
            }
        }

        protected Object resolveBeforeInstantiation(String beanName, BeanDefinition mbd) {
            Object bean = null;
            if (!Boolean.FALSE.equals(mbd.beforeInstantiationResolved)) {
                // Make sure bean class is actually resolved at this point.
                Class<?> targetType = resolveBeanClass(beanName, mbd, resourceLoader);
                if (targetType != null) {
                    //通知实例化前的PostProcessor事件
                    bean = applyBeanPostProcessorsBeforeInstantiation(targetType, beanName);
                    //如果事件后, 把实例返回了,则直接退出后续的所有生命周期逻辑(通常用于代理对象)
                    if (bean != null) {
                        //通知实例化后前的PostProcessor事件
                        bean = applyBeanPostProcessorsAfterInitialization(bean, beanName);
                    }
                }
            }
            return bean;
        }

        protected Object applyBeanPostProcessorsBeforeInstantiation(Class<?> beanClass, String beanName) {
            for (BeanPostProcessor bp : new ArrayList<>(beanPostProcessors)) {
                if (bp instanceof InstantiationAwareBeanPostProcessor) {
                    InstantiationAwareBeanPostProcessor ibp = (InstantiationAwareBeanPostProcessor) bp;
                    Object result = ibp.postProcessBeforeInstantiation(beanClass, beanName);
                    if (result != null) {
                        return result;
                    }
                }
            }
            return null;
        }

        protected Class resolveBeanClass(String beanName, BeanDefinition definition, Supplier<ClassLoader> loaderSupplier) {
            return definition.getBeanClassIfResolve(loaderSupplier);
        }

        /**
         * 创建bean实例, 分为无参创建或有参构造方法创建.
         *
         * @param beanName   bean名称
         * @param definition bean的定义描述
         * @param args       预期的构造入参,可能会改变
         * @return bean的实例包装
         */
        protected BeanWrapper createBeanInstance(String beanName, BeanDefinition definition, Object[] args) {
            Supplier<?> beanSupplier = definition.getBeanSupplier();
            Object beanInstance;
            //如果用户定义了实例的获取, 就使用户返回的.
            if (beanSupplier != null) {
                beanInstance = beanSupplier.get();
            } else {
                Class<?> beanClass = resolveBeanClass(beanName, definition, resourceLoader);
                //选出候选的构造方法,并排列好顺序, 如果需要用无参构造方法,则需要返回null.
                Constructor<?>[] ctors = determineConstructorsFromBeanPostProcessors(beanClass, beanName);
                if (ctors != null
                        || definition.getAutowireMode() == BeanDefinition.AUTOWIRE_CONSTRUCTOR
                        || definition.getConstructorArgumentValues().size() > 0
                        || (args != null && args.length > 0)) {
                    return autowireConstructor(beanName, definition, ctors, args);
                }
                //用无参构造创建实例
                beanInstance = newInstance(beanClass);
            }
            //创建包装bean, 包装bean可以方便的操作与配置bean的getter与setter,与参数类型转换.减少重复寻找class属性操作.
            BeanWrapper bw = new BeanWrapper(beanInstance);
            initBeanWrapper(bw);
            return bw;
        }

        protected BeanWrapper autowireConstructor(String beanName, BeanDefinition mbd, Constructor<?>[] ctors, Object[] explicitArgs) throws IllegalStateException {
            for (Constructor<?> constructor : ctors) {
                InjectElement<Constructor<?>> element = new InjectElement<>(constructor, ApplicationContext.this);
                try {
                    if (element.required == null) {
                        element.required = defaultInjectRequiredConstructor;
                    }
                    Object beanInstance = element.newInstance(explicitArgs);
                    if (beanInstance != null) {
                        BeanWrapper bw = new BeanWrapper(beanInstance);
                        initBeanWrapper(bw);
                        return bw;
                    }
                } catch (IllegalStateException e) {
                    //skip
                }
            }
            throw new IllegalStateException("can not create instances. " + Arrays.toString(ctors));
        }

        protected Constructor<?>[] determineConstructorsFromBeanPostProcessors(Class<?> beanClass, String beanName)
                throws RuntimeException {
            for (BeanPostProcessor bp : new ArrayList<>(beanPostProcessors)) {
                if (bp instanceof SmartInstantiationAwareBeanPostProcessor) {
                    SmartInstantiationAwareBeanPostProcessor ibp = (SmartInstantiationAwareBeanPostProcessor) bp;
                    Constructor<?>[] ctors = ibp.determineCandidateConstructors(beanClass, beanName);
                    if (ctors != null) {
                        return ctors;
                    }
                }
            }
            return null;
        }

        /**
         * 初始化包装bean
         *
         * @param bw 包装bean
         */
        protected void initBeanWrapper(BeanWrapper bw) {
            //自动注入需要的类型转换服务
            bw.conversionService = new ConversionService() {
            };
            //注册用户特殊的参数处理逻辑, 比如将原始数据是字符串'A,B,C',你可以注册一个字符串转数组的逻辑[A,B,C]
//            registerCustomEditors(bw);
            //实现需参照 org.springframework.beans.factory.support.AbstractBeanFactory.registerCustomEditors
        }

        protected void populateBean(String beanName, BeanDefinition definition, BeanWrapper bw) {
            //boolean控制是否需要继续填充bean的属性
            boolean continueWithPropertyPopulation = true;
            //通知实例化后的PostProcessor事件
            for (BeanPostProcessor bp : new ArrayList<>(beanPostProcessors)) {
                if (bp instanceof InstantiationAwareBeanPostProcessor) {
                    InstantiationAwareBeanPostProcessor ibp = (InstantiationAwareBeanPostProcessor) bp;
                    //注: 我这个写的自动注入是在这里实现的,与spring注入的时机不一样. 按照spring的写法代码量太大了.
                    if (!ibp.postProcessAfterInstantiation(bw.getWrappedInstance(), beanName)) {
                        continueWithPropertyPopulation = false;
                        break;
                    }
                }
            }
            if (!continueWithPropertyPopulation) {
                return;
            }

            //这里如果用户没有配置BeanDefinition,默认取出来的length是0
            PropertyValues pvs = definition.getPropertyValues();
            //如果用户在BeanDefinition中自己声明了自动注入,先把用户声明的注入进去
            if (definition.getAutowireMode() == BeanDefinition.AUTOWIRE_BY_NAME
                    || definition.getAutowireMode() == BeanDefinition.AUTOWIRE_BY_TYPE) {
                PropertyValues newPvs = new PropertyValues(pvs.getPropertyValues());
                if (definition.getAutowireMode() == BeanDefinition.AUTOWIRE_BY_NAME) {
                    autowireByName(beanName, definition, bw, newPvs);
                }
                if (definition.getAutowireMode() == BeanDefinition.AUTOWIRE_BY_TYPE) {
                    autowireByType(beanName, definition, bw, newPvs);
                }
                pvs = newPvs;
            }

            boolean needsDepCheck = definition.getDependencyCheck() != BeanDefinition.DEPENDENCY_CHECK_NONE;
            PropertyDescriptor[] filteredPds = null;
            //spring的自动注入是在这里的BeanPostProcessor和applyPropertyValues()方法实现的
            //注: 我自动注入没在这里实现,因为代码量太大了. 但和spring注入的处理逻辑是一致的,就是时机不一样,spring是分多批次注入,我是一次性注入
            for (BeanPostProcessor bp : new ArrayList<>(beanPostProcessors)) {
                //这里用户可以增加自己的自动注入属性, 用户返回PropertyValues即可.
                if (bp instanceof InstantiationAwareBeanPostProcessor) {
                    InstantiationAwareBeanPostProcessor ibp = (InstantiationAwareBeanPostProcessor) bp;
                    //如果用户返回的PropertyValues是null, 让用户消费掉之前的PropertyValues
                    PropertyValues pvsToUse = ibp.postProcessProperties(pvs, bw.getWrappedInstance(), beanName);
                    if (pvsToUse == null) {
                        if (filteredPds == null) {
                            filteredPds = filterPropertyDescriptorsForDependencyCheck(bw, definition.allowCaching);
                        }
                        pvsToUse = ibp.postProcessPropertyValues(pvs, filteredPds, bw.getWrappedInstance(), beanName);
                        if (pvsToUse == null) {
                            return;
                        }
                    }
                    pvs = pvsToUse;
                }
            }
            //如果需要检查循环依赖
            if (needsDepCheck) {
                if (filteredPds == null) {
                    //过滤掉一些不检查循环依赖的属性
                    filteredPds = filterPropertyDescriptorsForDependencyCheck(bw, definition.allowCaching);
                }
                //检查循环依赖
                checkDependencies(beanName, definition, filteredPds, pvs);
            }

            if (pvs != null) {
                //这里为了处理InstantiationAwareBeanPostProcessor的返回结果pvs.没人实现的话pvs.length应该是0
                applyPropertyValues(beanName, definition, bw, pvs);
            }
        }

        protected void checkDependencies(String beanName, BeanDefinition mbd, PropertyDescriptor[] pds, PropertyValues pvs)
                throws IllegalStateException {
            int dependencyCheck = mbd.getDependencyCheck();
            for (PropertyDescriptor pd : pds) {
                if (pd.getWriteMethod() != null && (pvs == null || !pvs.contains(pd.getName()))) {
                    boolean isSimple = isSimpleProperty(pd.getPropertyType());
                    boolean unsatisfied = (dependencyCheck == BeanDefinition.DEPENDENCY_CHECK_ALL) ||
                            (isSimple && dependencyCheck == BeanDefinition.DEPENDENCY_CHECK_SIMPLE) ||
                            (!isSimple && dependencyCheck == BeanDefinition.DEPENDENCY_CHECK_OBJECTS);
                    if (unsatisfied) {
                        throw new IllegalStateException("Set this property value or disable dependency checking for this bean.");
                    }
                }
            }
        }

        protected PropertyDescriptor[] filterPropertyDescriptorsForDependencyCheck(BeanWrapper bw, boolean cache) {
            PropertyDescriptor[] filtered = this.filteredPropertyDescriptorsCache.get(bw.getWrappedClass());
            if (filtered == null) {
                filtered = bw.getPropertyDescriptors();
                if (cache) {
                    PropertyDescriptor[] existing =
                            this.filteredPropertyDescriptorsCache.putIfAbsent(bw.getWrappedClass(), filtered);
                    if (existing != null) {
                        filtered = existing;
                    }
                }
            }
            return filtered;
        }

        public Object applyBeanPostProcessorsAfterInitialization(Object existingBean, String beanName)
                throws RuntimeException {
            Object result = existingBean;
            for (BeanPostProcessor processor : new ArrayList<>(beanPostProcessors)) {
                Object current = processor.postProcessAfterInitialization(result, beanName);
                if (current == null) {
                    return result;
                }
                result = current;
            }
            return result;
        }

        protected void applyPropertyValues(String beanName, BeanDefinition definition, BeanWrapper bw, PropertyValues pvs) {
            bw.setPropertyValues(pvs);
        }

        private <T> T newInstance(Class<T> clazz) throws IllegalStateException {
            try {
                Object instance = clazz.getDeclaredConstructor().newInstance();
                return (T) instance;
            } catch (Exception e) {
                throw new IllegalStateException("newInstanceByJdk error=" + e, e);
            }
        }

        private void autowireByType(String beanName, BeanDefinition definition, BeanWrapper beanInstanceWrapper, PropertyValues pvs) {
        }

        private void autowireByName(String beanName, BeanDefinition definition, BeanWrapper beanInstanceWrapper, PropertyValues pvs) {
        }
    }
}