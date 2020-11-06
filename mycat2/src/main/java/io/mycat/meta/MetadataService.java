package io.mycat.meta;

/**
 * 统一管理存储(持久化,内存,过期时间等,第三方存储), 后期做UI页面可以对元数据增删改查
 * 前端数据(用户连接),
 * 后端数据(mysql),
 * 代理数据(mycat)
 * @author wangzihaogithub 2020年4月19日10:23:11
 */
public interface MetadataService {
    BackendMetadata getBackendMetadata();
    FrontendMetadata getFrontendMetadata();
    ProxyMetadata getProxyMetadata();

    BackendMetadata getBackendMetadata(Object id);
    FrontendMetadata getFrontendMetadata(Object id);
    ProxyMetadata getProxyMetadata(Object id);

    interface FrontendMetadata extends Metadata{
    }

    interface ProxyMetadata extends Metadata{
    }

    interface BackendMetadata extends Metadata{
    }

    interface Metadata {
        <T>T getValue(String key);
        <T>T setValue(String key,T value);
    }
}
