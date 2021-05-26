package Hive;

import org.stringtemplate.v4.ST;

import java.sql.*;

public class create {
    public static String driverName = "org.apache.hive.jdbc.HiveDriver";//数据库驱动
    public static String user = "root";
    public static String password = "south189";
    public static void main(String[] args) {
        try {
            Class.forName(driverName);//驱动设置
            Connection con = DriverManager.getConnection("jdbc:hive2://server1:10000", user, password);//database地址，账号与密码
            Statement statement = con.createStatement();//连接
            String databaseName = "study";
//            String tableName = "students";
            String tableName = "person";
            //1.插入数据
//            String sql = "insert into table "+databaseName+"."+tableName+" values(24,'cjdan',20,'male',173)";//报错
//            String sql = "insert into table "+databaseName+"."+tableName+" select 24,'cjdan',20,'male',173";//插入数据，values不能用，用select
//            System.out.println("插入数据");
//            statement.execute(sql);
//            System.out.println("插入数据成功");
            //2.创建表并指定表文件的存放路径
//            String sql = "create  table if not exists stu2(id int ,name string) row format delimited fields terminated by '\t' location '/usr/local/data'";
//            statement.execute(sql);
            //3.根据查询结果创建表
//            String sql = "create table "+ databaseName+"."+tableName1+ " as select * from "+ databaseName+"."+tableName;
//            statement.execute(sql);
            //4.根据已经存在的表结构创建表

//            String sql = "create table "+ databaseName+".business like default.business";
//            statement.execute(sql);
            //5.外部表(external table)
//            String sql = "create external table external_table (name string,language string) location '/user/south/input/Text.txt'";
//            statement.execute(sql);

            //6.分桶表操作
                /*
                6.1 开启 Hive 的分桶功能,set hive.enforce.bucketing=true;
                6.2 设置 Reduce 个数,set mapreduce.job.reduces=3;
                 */
//            int num = 3;//桶的个数
////            String sql ="create table if not exists " +databaseName
////                    +"."+tableName+
////                    " ( id int comment '编号', name varchar(200) COMMENT '姓名'," +
////                    "age int COMMENT '年龄',gender varchar(200) comment '性别'," +
////                    "height int comment '身高',data varchar(200) comment '日期')" +
////                    " clustered by (id) into " +num+
////                    " buckets row format delimited fields terminated by ','";
////            statement.execute(sql);
////            sql = "insert into "+databaseName+"."+tableName+" select * from study.people";
////            System.out.println("sql = "+sql);
////            statement.execute(sql);

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }
}
