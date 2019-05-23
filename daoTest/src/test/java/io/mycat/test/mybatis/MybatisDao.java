package io.mycat.test.mybatis;

import io.mycat.test.pojo.TravelRecord;
import java.io.IOException;
import org.apache.ibatis.session.SqlSession;

/**
 * @author jamie12221
 * @date 2019-05-19 18:02
 **/
public class MybatisDao {

  public static DataConnection dataConn = new DataConnection();

  public static void main(String[] args) throws IOException {
    try (SqlSession sqlSession = dataConn.getSqlSession()) {
      while (true) {
        sqlSession.insert("test.insertTravelRecord", new TravelRecord());
        Object o = sqlSession.selectOne("test.findTravelRecordById", 1);
        sqlSession.commit();
      }
    }
  }

}
