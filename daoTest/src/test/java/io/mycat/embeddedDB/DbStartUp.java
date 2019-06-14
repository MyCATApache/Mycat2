package io.mycat.embeddedDB;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;

public class DbStartUp {

  final static String source = "CREATE TABLE `travelrecord` (\n"
      + "  `id` bigint(20) NOT NULL,\n"
      + "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n"
      + "  `traveldate` date DEFAULT NULL,\n"
      + "  `fee` decimal(10,0) DEFAULT NULL,\n"
      + "  `days` int(11) DEFAULT NULL\n"
      + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
      + "\n"
      + "\n"
      + "CREATE TABLE `travelrecord2` (\n"
      + "  `id` bigint(20) NOT NULL,\n"
      + "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n"
      + "  `traveldate` date DEFAULT NULL,\n"
      + "  `fee` decimal(10,0) DEFAULT NULL,\n"
      + "  `days` int(11) DEFAULT NULL\n"
      + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
      + "\n"
      + "\n"
      + "CREATE TABLE `travelrecord3` (\n"
      + "  `id` bigint(20) NOT NULL,\n"
      + "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n"
      + "  `traveldate` date DEFAULT NULL,\n"
      + "  `fee` decimal(10,0) DEFAULT NULL,\n"
      + "  `days` int(11) DEFAULT NULL\n"
      + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n";

  public static void start() throws ManagedProcessException {
    startMySQLServer(3307);
    startMySQLServer(3308);
  }


  public static void startMySQLServer(int port) throws ManagedProcessException {
    DB db = DB.newEmbeddedDB(port);
    db.start();
    db.createDB("db1");
    db.run("use db1;" + source);
    db.createDB("db2");
    db.run("use db2;" + source);
    db.createDB("db3");
    db.run("use db3;" + source);
  }

}