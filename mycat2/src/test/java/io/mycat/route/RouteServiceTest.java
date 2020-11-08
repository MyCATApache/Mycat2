//package io.mycat.route;
//
//import io.mycat.hbt.ast.base.Schema;
//import io.mycat.metadata.MetadataManager;
//import org.jetbrains.annotations.NotNull;
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.util.Optional;
//
//public class RouteServiceTest {
//    RouteService routeService = RouteService.create(MetadataManager.INSTANCE);
//    @Test
//    public void test(){
//        String sql = "select 1";
//        String text = route(sql);
//        Assert.assertSame(text,"");
//    }
//
//    @NotNull
//    private String route(String sql) {
//        return Optional.ofNullable(routeService.route(sql)).map(i->i.toString()).orElse("");
//    }
//}
