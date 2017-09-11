package io.mycat.mycat2.ymlTest;

import io.mycat.util.YamlUtil;

import java.io.FileNotFoundException;

/**
 * Desc:
 *
 * @date: 09/09/2017
 * @author: gaozhiwen
 */
public class DataSourceTest {
    public static void main(String[] args) throws FileNotFoundException {
        RepListBean bean = YamlUtil.load("datasource.yml", RepListBean.class);
        System.out.println(bean);

        String str = YamlUtil.dump(bean);
        System.out.println(str);
    }
}
