package io.mycat.expression;

import java.util.ArrayList;
import java.util.List;

public class ExprSession {

  //用于创建子任务
  final List<ExprSession> subSessionList = new ArrayList<>(0);
  //用于保存临时数据
  final SessionData sessionData = new SessionData(this);
}