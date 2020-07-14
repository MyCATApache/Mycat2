package io.mycat.testsuite;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;

public class TestConnectionFactory extends AbstractTestConnectionFactory{
    public TestConnectionFactory() {
        super(()->{
            return new SimpleConnection() {
                @Override
                public ResultSet executeQuery(String sql) {
                    return new ResultSet() {
                        @Override
                        public List<String> getColumnList() {
                            return Arrays.asList("1");
                        }

                        @Override
                        public List<Object[]> getRows() {
                            return ImmutableList.of(new Object[]{"1"});
                        }
                    };
                }

                @Override
                public void useSchema(String schema) {

                }
            };
        });
    }
}