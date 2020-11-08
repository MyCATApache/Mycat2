package io.mycat.metadata;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.NamedMethodGenerator;
import io.mycat.util.NameMap;

public class ServiceTable {
   final NameMap< NameMap<CustomTableHandler>> map = new NameMap<>();
    final boolean caseSensitive;

    public ServiceTable(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }



   public void addTable(CustomTableHandler customTableHandler){

   }
}
