package io.mycat.config;

import io.mycat.config.DatasourceConfig;

import java.util.Map;
import java.util.function.Supplier;

public interface DatasourceConfigProvider extends Supplier< Map<String, DatasourceConfig>> {

}