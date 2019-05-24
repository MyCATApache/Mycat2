package io.mycat.test.mybatis;

import java.io.IOException;
import java.io.InputStream;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

public class DataConnection {

  private String resource = "io/mycat/test/mybatis/SqlMapConfig.xml";
  private SqlSessionFactory sqlSessionFactory;
  private SqlSession sqlSession;

  public SqlSession getSqlSession() throws IOException {
    InputStream inputStream = Resources.getResourceAsStream(resource);
    sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
    sqlSession = sqlSessionFactory.openSession();
    return sqlSession;
  }
}