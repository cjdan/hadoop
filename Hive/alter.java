package Hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class alter {
    public static String driverName = "org.apache.hive.jdbc.HiveDriver";//数据库驱动
    public static String user = "root";
    public static String password = "south189";
    public static void main(String[] args) {
        try {
            Class.forName(driverName);//驱动设置
            Connection con = DriverManager.getConnection("jdbc:hive2://server1:10000", user, password);//database地址，账号与密码
            Statement statement = con.createStatement();//连接

            //1.修改表名
//            String sql = "alter  table  study.text  rename  to  study.test";
//            statement.execute(sql);
            //2.增加/修改列信息
            //2.1 添加列
//            String sql = "alter  table study.test add columns (mycol string, mysco int)";
//            statement.execute(sql);
            //2.1 更新列
            String sql = "alter table study.test change column mysco mysconew string";
            statement.execute(sql);


        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

    }
}
