package io.mycat.ui;

import java.util.Map;

public interface InfoProviderFactory {

    InfoProvider create(InfoProviderType type, Map<String,String> args);
}
