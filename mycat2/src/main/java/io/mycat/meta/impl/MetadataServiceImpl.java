package io.mycat.meta.impl;

import io.mycat.meta.MetadataService;


import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 统一管理存储(持久化,内存,过期时间等,第三方存储), 后期做UI页面可以对元数据增删改查
 * 前端数据(用户连接),
 * 后端数据(mysql),
 * 代理数据(mycat)
 * @author wangzihaogithub 2020年4月19日10:23:11
 */

public class MetadataServiceImpl implements MetadataService {
    private final ProxyMetadata globalProxyMetadata = new GlobalProxyMetadataImpl();
    private final FrontendMetadata globalFrontendMetadata = new GlobalFrontendMetadataImpl();
    private final BackendMetadata globalBackendMetadata = new GlobalBackendMetadataImpl();

    private final Map<Object,ProxyMetadata> sessionProxyMetadataMap = new ConcurrentHashMap<>();
    private final Map<Object,FrontendMetadata> sessionFrontendMetadataMap = new ConcurrentHashMap<>();
    private final Map<Object,BackendMetadata> sessionBackendMetadataMap = new ConcurrentHashMap<>();

    @Override
    public BackendMetadata getBackendMetadata() {
        return globalBackendMetadata;
    }

    @Override
    public FrontendMetadata getFrontendMetadata() {
        return globalFrontendMetadata;
    }

    @Override
    public ProxyMetadata getProxyMetadata() {
        return globalProxyMetadata;
    }

    @Override
    public BackendMetadata getBackendMetadata(Object id) {
        BackendMetadata backendMetadata = sessionBackendMetadataMap.computeIfAbsent(id, o -> new SessionBackendMetadataImpl());
        return backendMetadata;
    }

    @Override
    public FrontendMetadata getFrontendMetadata(Object id) {
        FrontendMetadata backendMetadata = sessionFrontendMetadataMap.computeIfAbsent(id, o -> new SessionFrontendMetadataImpl());
        return backendMetadata;
    }

    @Override
    public ProxyMetadata getProxyMetadata(Object id) {
        ProxyMetadata backendMetadata = sessionProxyMetadataMap.computeIfAbsent(id, o -> new SessionProxyMetadataImpl());
        return backendMetadata;
    }


    protected static abstract class AbstractMetadata extends ConcurrentHashMap<String,Object> implements Metadata{
        @Override
        public <T> T getValue(String key) {
            return (T) get(key);
        }

        @Override
        public <T> T setValue(String key, T value) {
            return (T) put(key,value);
        }
    }

    protected static abstract class AbstractSessionMetadata extends WeakHashMap<String,Object> implements Metadata{
        @Override
        public <T> T getValue(String key) {
            return (T) get(key);
        }

        @Override
        public <T> T setValue(String key, T value) {
            return (T) put(key,value);
        }
    }

    private static class SessionProxyMetadataImpl extends AbstractSessionMetadata implements ProxyMetadata{

    }
    private static class SessionFrontendMetadataImpl extends AbstractSessionMetadata implements FrontendMetadata{

    }
    private static class SessionBackendMetadataImpl extends AbstractSessionMetadata implements BackendMetadata {

    }

    private static class GlobalProxyMetadataImpl extends AbstractMetadata implements ProxyMetadata{

    }
    private static class GlobalFrontendMetadataImpl extends AbstractMetadata implements FrontendMetadata{

    }
    private static class GlobalBackendMetadataImpl extends AbstractMetadata implements BackendMetadata {

    }

}
