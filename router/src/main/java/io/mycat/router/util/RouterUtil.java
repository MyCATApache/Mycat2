package io.mycat.router.util;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.wall.spi.WallVisitorUtils;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 从ServerRouterUtil中抽取的一些公用方法，路由解析工具类
 * @author wang.dw
 *
 */
public class RouterUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(RouterUtil.class);

	/**
	 * 移除执行语句中的数据库名
	 *
	 * @param stmt		 执行语句
	 * @param schema  	数据库名
	 * @return 			执行语句
	 * @author mycat
     *
     * @modification 修正移除schema的方法
   *  date 2016/12/29
     * @modifiedBy Hash Zhang
     *
	 */
	public static String removeSchema(String stmt, String schema) {
        final String upStmt = stmt.toUpperCase();
        final String upSchema = schema.toUpperCase() + ".";
        final String upSchema2 = new StringBuilder("`").append(schema.toUpperCase()).append("`.").toString();
        int strtPos = 0;
        int indx = 0;

        int indx1 = upStmt.indexOf(upSchema, strtPos);
        int indx2 = upStmt.indexOf(upSchema2, strtPos);
        boolean flag = indx1 < indx2 ? indx1 == -1 : indx2 != -1;
        indx = !flag ? indx1 > 0 ? indx1 : indx2 : indx2 > 0 ? indx2 : indx1;
        if (indx < 0) {
            return stmt;
        }

        int firstE = upStmt.indexOf("'");
        int endE = upStmt.lastIndexOf("'");

        StringBuilder sb = new StringBuilder();
        while (indx > 0) {
          sb.append(stmt, strtPos, indx);

            if (flag) {
                strtPos = indx + upSchema2.length();
            } else {
                strtPos = indx + upSchema.length();
            }
            if (indx > firstE && indx < endE && countChar(stmt, indx) % 2 == 1) {
              sb.append(stmt, indx, indx + schema.length() + 1);
            }
            indx1 = upStmt.indexOf(upSchema, strtPos);
            indx2 = upStmt.indexOf(upSchema2, strtPos);
            flag = indx1 < indx2 ? indx1 == -1 : indx2 != -1;
            indx = !flag ? indx1 > 0 ? indx1 : indx2 : indx2 > 0 ? indx2 : indx1;
        }
        sb.append(stmt.substring(strtPos));
        return sb.toString();
    }

	private static int countChar(String sql,int end)
	{
		int count=0;
		boolean skipChar = false;
		for (int i = 0; i < end; i++) {
      if (sql.charAt(i) == '\'' && !skipChar) {
        count++;
        skipChar = false;
      } else {
        skipChar = sql.charAt(i) == '\\';
      }
		}
		return count;
	}

	/**
	 * 处理SQL
	 *
	 * @param stmt   执行语句
	 * @return 		 处理后SQL
	 * @author AStoneGod
	 */
	public static String getFixedSql(String stmt){
		stmt = stmt.replaceAll("\r\n", " "); //对于\r\n的字符 用 空格处理 rainbow
		return stmt = stmt.trim(); //.toUpperCase();
	}

	/**
	 * 获取table名字
	 *
	 * @param stmt  	执行语句
	 * @param repPos	开始位置和位数
	 * @return 表名
	 * @author AStoneGod
	 */
	public static String getTableName(String stmt, int[] repPos) {
		int startPos = repPos[0];
		int secInd = stmt.indexOf(' ', startPos + 1);
		if (secInd < 0) {
			secInd = stmt.length();
		}
		int thiInd = stmt.indexOf('(',secInd+1);
		if (thiInd < 0) {
			thiInd = stmt.length();
		}
		repPos[1] = secInd;
		String tableName = "";
		if (stmt.toUpperCase().startsWith("DESC")||stmt.toUpperCase().startsWith("DESCRIBE")){
			tableName = stmt.substring(startPos, thiInd).trim();
		}else {
			tableName = stmt.substring(secInd, thiInd).trim();
		}

		//ALTER TABLE
		if (tableName.contains(" ")){
			tableName = tableName.substring(0,tableName.indexOf(" "));
		}
		int ind2 = tableName.indexOf('.');
		if (ind2 > 0) {
			tableName = tableName.substring(ind2 + 1);
		}
		return tableName;
	}


	/**
	 * 获取show语句table名字
	 *
	 * @param stmt	        执行语句
	 * @param repPos   开始位置和位数
	 * @return 表名
	 * @author AStoneGod
	 */
	public static String getShowTableName(String stmt, int[] repPos) {
		int startPos = repPos[0];
		int secInd = stmt.indexOf(' ', startPos + 1);
		if (secInd < 0) {
			secInd = stmt.length();
		}

		repPos[1] = secInd;
		String tableName = stmt.substring(startPos, secInd).trim();

		int ind2 = tableName.indexOf('.');
		if (ind2 > 0) {
			tableName = tableName.substring(ind2 + 1);
		}
		return tableName;
	}

	/**
	 * 获取语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt     执行语句
	 * @param start      开始位置
	 * @return int[]	  关键字位置和占位个数
	 *
	 * @author mycat
	 *
	 * @modification 修改支持语句中包含“IF NOT EXISTS”的情况
   *  date 2016/12/8
	 * @modifiedBy Hash Zhang
	 */
	public static int[] getCreateTablePos(String upStmt, int start) {
		String token1 = "CREATE ";
		String token2 = " TABLE ";
		String token3 = " EXISTS ";
		int createInd = upStmt.indexOf(token1, start);
		int tabInd1 = upStmt.indexOf(token2, start);
		int tabInd2 = upStmt.indexOf(token3, tabInd1);
		// 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
		if (createInd >= 0 && tabInd2 > 0 && tabInd2 > createInd) {
			return new int[] { tabInd2, token3.length() };
		} else if(createInd >= 0 && tabInd1 > 0 && tabInd1 > createInd) {
			return new int[] { tabInd1, token2.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

	/**
	 * 获取语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt
	 *            执行语句
	 * @param start
	 *            开始位置
	 * @return int[]关键字位置和占位个数
	 * @author aStoneGod
	 */
	public static int[] getCreateIndexPos(String upStmt, int start) {
		String token1 = "CREATE ";
		String token2 = " INDEX ";
		String token3 = " ON ";
		int createInd = upStmt.indexOf(token1, start);
		int idxInd = upStmt.indexOf(token2, start);
		int onInd = upStmt.indexOf(token3, start);
		// 既包含CREATE又包含INDEX，且CREATE关键字在INDEX关键字之前, 且包含ON...
		if (createInd >= 0 && idxInd > 0 && idxInd > createInd && onInd > 0 && onInd > idxInd) {
			return new int[] {onInd , token3.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

	/**
	 * 获取ALTER语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt   执行语句
	 * @param start    开始位置
	 * @return int[]   关键字位置和占位个数
	 * @author aStoneGod
	 */
	public static int[] getAlterTablePos(String upStmt, int start) {
		String token1 = "ALTER ";
		String token2 = " TABLE ";
		int createInd = upStmt.indexOf(token1, start);
		int tabInd = upStmt.indexOf(token2, start);
		// 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
		if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
			return new int[] { tabInd, token2.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

	/**
	 * 获取DROP语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt 	执行语句
	 * @param start  	开始位置
	 * @return int[]	关键字位置和占位个数
	 * @author aStoneGod
	 */
	public static int[] getDropTablePos(String upStmt, int start) {
		//增加 if exists判断
		if(upStmt.contains("EXISTS")){
			String token1 = "IF ";
			String token2 = " EXISTS ";
			int ifInd = upStmt.indexOf(token1, start);
			int tabInd = upStmt.indexOf(token2, start);
			if (ifInd >= 0 && tabInd > 0 && tabInd > ifInd) {
				return new int[] { tabInd, token2.length() };
			} else {
				return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
			}
		}else {
			String token1 = "DROP ";
			String token2 = " TABLE ";
			int createInd = upStmt.indexOf(token1, start);
			int tabInd = upStmt.indexOf(token2, start);

			if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
				return new int[] { tabInd, token2.length() };
			} else {
				return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
			}
		}
	}


	/**
	 * 获取DROP语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt
	 *            执行语句
	 * @param start
	 *            开始位置
	 * @return int[]关键字位置和占位个数
	 * @author aStoneGod
	 */

	public static int[] getDropIndexPos(String upStmt, int start) {
		String token1 = "DROP ";
		String token2 = " INDEX ";
		String token3 = " ON ";
		int createInd = upStmt.indexOf(token1, start);
		int idxInd = upStmt.indexOf(token2, start);
		int onInd = upStmt.indexOf(token3, start);
		// 既包含CREATE又包含INDEX，且CREATE关键字在INDEX关键字之前, 且包含ON...
		if (createInd >= 0 && idxInd > 0 && idxInd > createInd && onInd > 0 && onInd > idxInd) {
			return new int[] {onInd , token3.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

	/**
	 * 获取TRUNCATE语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt    执行语句
	 * @param start     开始位置
	 * @return int[]	关键字位置和占位个数
	 * @author aStoneGod
	 */
	public static int[] getTruncateTablePos(String upStmt, int start) {
		String token1 = "TRUNCATE ";
		String token2 = " TABLE ";
		int createInd = upStmt.indexOf(token1, start);
		int tabInd = upStmt.indexOf(token2, start);
		// 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
		if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
			return new int[] { tabInd, token2.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

	/**
	 * 获取语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt   执行语句
	 * @param start    开始位置
	 * @return int[]   关键字位置和占位个数
	 * @author mycat
	 */
	public static int[] getSpecPos(String upStmt, int start) {
		String token1 = " FROM ";
		String token2 = " IN ";
		int tabInd1 = upStmt.indexOf(token1, start);
		int tabInd2 = upStmt.indexOf(token2, start);
		if (tabInd1 > 0) {
			if (tabInd2 < 0) {
				return new int[] { tabInd1, token1.length() };
			}
			return (tabInd1 < tabInd2) ? new int[] { tabInd1, token1.length() }
					: new int[] { tabInd2, token2.length() };
		} else {
			return new int[] { tabInd2, token2.length() };
		}
	}

	/**
	 * 获取开始位置后的 LIKE、WHERE 位置 如果不含 LIKE、WHERE 则返回执行语句的长度
	 *
	 * @param upStmt   执行sql
	 * @param start    开始位置
	 * @return int
	 * @author mycat
	 */
	public static int getSpecEndPos(String upStmt, int start) {
		int tabInd = upStmt.toUpperCase().indexOf(" LIKE ", start);
		if (tabInd < 0) {
			tabInd = upStmt.toUpperCase().indexOf(" WHERE ", start);
		}
		if (tabInd < 0) {
			return upStmt.length();
		}
		return tabInd;
	}

	/*
	 *  找到返回主键的的位置 
	 *  找不到返回 -1
	 * */
	private static int isPKInFields(String origSQL,String primaryKey,int firstLeftBracketIndex,int firstRightBracketIndex){

		if (primaryKey == null) {
			throw new RuntimeException("please make sure the primaryKey's config is not null in schemal.xml");
		}

		boolean isPrimaryKeyInFields = false;
		int  pkStart = 0;
		String upperSQL = origSQL.substring(firstLeftBracketIndex, firstRightBracketIndex + 1).toUpperCase();
		for (int pkOffset = 0, primaryKeyLength = primaryKey.length();;) {
			pkStart = upperSQL.indexOf(primaryKey, pkOffset);
			if (pkStart >= 0 && pkStart < firstRightBracketIndex) {
				char pkSide = upperSQL.charAt(pkStart - 1);
				if (pkSide <= ' ' || pkSide == '`' || pkSide == ',' || pkSide == '(') {
					pkSide = upperSQL.charAt(pkStart + primaryKey.length());
					isPrimaryKeyInFields = pkSide <= ' ' || pkSide == '`' || pkSide == ',' || pkSide == ')';
				}
				if (isPrimaryKeyInFields) {
					break;
				}
				pkOffset = pkStart + primaryKeyLength;
			} else {
				break;
			}
		}
		if (isPrimaryKeyInFields) {
			return firstLeftBracketIndex + pkStart;
		} else {
			return  -1;
		}
		
	}


	public static List<String> handleBatchInsert(String origSQL, int valuesIndex) {
		List<String> handledSQLs = new LinkedList<>();
		String prefix = origSQL.substring(0, valuesIndex + "VALUES".length());
		String values = origSQL.substring(valuesIndex + "VALUES".length());
		int flag = 0;
		StringBuilder currentValue = new StringBuilder();
		currentValue.append(prefix);
		for (int i = 0; i < values.length(); i++) {
			char j = values.charAt(i);
			if (j == '(' && flag == 0) {
				flag = 1;
				currentValue.append(j);
			} else if (j == '\"' && flag == 1) {
				flag = 2;
				currentValue.append(j);
			} else if (j == '\'' && flag == 1) {
				flag = 3;
				currentValue.append(j);
			} else if (j == '\\' && flag == 2) {
				flag = 4;
				currentValue.append(j);
			} else if (j == '\\' && flag == 3) {
				flag = 5;
				currentValue.append(j);
			} else if (flag == 4) {
				flag = 2;
				currentValue.append(j);
			} else if (flag == 5) {
				flag = 3;
				currentValue.append(j);
			} else if (j == '\"' && flag == 2) {
				flag = 1;
				currentValue.append(j);
			} else if (j == '\'' && flag == 3) {
				flag = 1;
				currentValue.append(j);
			} else if (j == ')' && flag == 1) {
				flag = 0;
				currentValue.append(j);
				handledSQLs.add(currentValue.toString());
				currentValue = new StringBuilder();
				currentValue.append(prefix);
			} else if (j == ',' && flag == 0) {
				continue;
			} else {
				currentValue.append(j);
			}
		}
		return handledSQLs;
	}
	 /**
	  * 对于插入的sql : "insert into hotnews(title,name) values('test1',\"name\"),('(test)',\"(test)\"),('\\\"',\"\\'\"),(\")\",\"\\\"\\')\")"：
	  *  需要返回结果：
	  *[[ 'test1', "name"],
	  *	['(test)', "(test)"],
	  *	['\"', "\'"],
	  *	[")", "\"\')"],
	  *	[ 1,  null]
	  * 值结果的解析
	  */	
    public  static List<List<String>> parseSqlValue(String origSQL,int valuesIndex ) {
        List<List<String>> valueArray = new ArrayList<>();
        String valueStr = origSQL.substring(valuesIndex + 6);// 6 values 长度为6
        String preStr = origSQL.substring(0, valuesIndex );// 6 values 长度为6
        int pos = 0 ;
        int flag  = 4;
        int len = valueStr.length();
        StringBuilder currentValue = new StringBuilder();
//        int colNum = 2; //
        char c ;
        List<String> curList = new ArrayList<>();
		int parenCount = 0;
        for( ;pos < len; pos ++) {
            c = valueStr.charAt(pos);
            if(flag == 1  || flag == 2) {
                currentValue.append(c);
                if(c == '\\') {
                    char nextCode = valueStr.charAt(pos + 1);
                    if(nextCode == '\'' || nextCode == '\"') {
                        currentValue.append(nextCode);
                        pos++;
                        continue;
                    }
                }
                if(c == '\"' && flag == 1) {
                    flag = 0;
                    continue;
                }
                if(c == '\'' && flag == 2) {
                    flag = 0;
                    continue;
                }
            }  else if(c == '\"'){
                currentValue.append(c);
                flag = 1;
            } else if (c == '\'') {
                currentValue.append(c);
                flag = 2;
            } else if (c == '(') {
            	if (flag == 4) {
					curList = new ArrayList<>();
					flag = 0;
				} else {
					currentValue.append(c);
					flag = 6;
					parenCount++;
				}
            } else if (flag == 4) {
				continue;
			} else if (flag == 6) {
				currentValue.append(c);
				if (c == '(') {
					parenCount++;
				} else if (c == ')') {
					parenCount--;
				}
				if (parenCount == 0) {
					flag = 0;
				}
			} else if(c == ',') {
//                System.out.println(currentValue);
                curList.add(currentValue.toString());
                currentValue.delete(0, currentValue.length());
            } else if(c == ')'){
				flag = 4;
//                System.out.println(currentValue);
				curList.add(currentValue.toString());
				currentValue.delete(0, currentValue.length());
				valueArray.add(curList);
            }  else {
                currentValue.append(c);
            }
        }
        return valueArray;
    }  




	/**
	 * 系统表判断,某些sql语句会查询系统表或者跟系统表关联
	 * @author lian
   *  date 2016年12月2日
	 * @param tableName
	 * @return
	 */
	public static boolean isSystemSchema(String tableName) {
		// 以information_schema， mysql开头的是系统表
    return tableName.startsWith("INFORMATION_SCHEMA.")
               || tableName.startsWith("MYSQL.")
               || tableName.startsWith("PERFORMANCE_SCHEMA.");

  }

	/**
	 * 判断条件是否永真
	 * @param expr
	 * @return
	 */
	public static boolean isConditionAlwaysTrue(SQLExpr expr) {
		Object o = WallVisitorUtils.getValue(expr);
    return Boolean.TRUE.equals(o);
  }

	/**
	 * 判断条件是否永假的
	 * @param expr
	 * @return
	 */
	public static boolean isConditionAlwaysFalse(SQLExpr expr) {
		Object o = WallVisitorUtils.getValue(expr);
    return Boolean.FALSE.equals(o);
  }

	/**
	 * 寻找joinKey的索引
	 *
	 * @param columns
	 * @param joinKey
	 * @return -1表示没找到，>=0表示找到了
	 */
	private static int getJoinKeyIndex(List<SQLExpr> columns, String joinKey) {
		for (int i = 0; i < columns.size(); i++) {
			String col = StringUtil.removeBackquote(columns.get(i).toString()).toUpperCase();
			if (col.equals(joinKey)) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * 是否为批量插入：insert into ...values (),()...或 insert into ...select.....
	 *
	 * @param insertStmt
	 * @return
	 */
	private static boolean isMultiInsert(MySqlInsertStatement insertStmt) {
		return (insertStmt.getValuesList() != null && insertStmt.getValuesList().size() > 1)
				|| insertStmt.getQuery() != null;
	}

}
