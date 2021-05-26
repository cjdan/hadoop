package Hive;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class HiveDemo {
    public static String driverName = "org.apache.hive.jdbc.HiveDriver";//数据库驱动


    public static void main(String[] args) throws SQLException {
        String user = "root";
        String password = "south189";

        try {
            Class.forName(driverName);//驱动设置
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        try {
            Connection con = DriverManager.getConnection("jdbc:hive2://server1:10000", user, password);//database地址，账号与密码
            //本地路径
            String localFilePath = "/usr/local/data/student.csv";
            Statement statement = con.createStatement();//连接
            String databaseName = "study";
            String tableName = "person";
            String databaseSQL = "create database if not exists " + databaseName;
//            readLocalFile(statement,databaseName,tableName,localFilePath);
//            selectTable(statement,databaseName,tableName,12);
            dropTable(statement,databaseName,tableName);



//            Map<String, String> columns =new HashMap<>();
//            columns.put("weight","int");
//            columns.put("weight","int");
//            createDatabase(statement,databaseSQL);
//            showDatabases(statement);

//            showTables(statement,);
//            String tableSQL = "create table if NOT exists "+databaseName+"."+tableName+" (id int comment '编号', " +
//                    "name varchar(200) COMMENT '姓名',age int COMMENT '年龄',gender varchar(200) comment '性别'," +
//                    "height int comment '身高') partitioned by(data string) row format delimited fields terminated by ','";
//            System.out.println(tableSQL);
//            createTable(statement, tableSQL);
//            descTable(statement,databaseName,tableName);
//            addColumns(statement,databaseName,tableName,columns);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("连接不成功~，请检查");
        }
    }

    //创建数据库
    public static void createDatabase(Statement statement,String databaseSQL) throws SQLException {
        try {
            statement.execute(databaseSQL);
            System.out.println("创建数据库成功");
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println("创建数据库不成功，请检查");
        }
    }

    //删除数据库
    public static void dropDatabase(Statement statement,String databaseName) throws SQLException {
        try {
            String sql = "drop database if exists "+databaseName;
            statement.execute(sql);
            System.out.println("删除数据库成功");
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println("删除数据库不成功，请检查");
        }
    }

    //创建表
    public static void createTable(Statement statement,String tableSQL) throws SQLException {
        try {
//            statement.execute("use "+databaseName);
            statement.execute(tableSQL);
            System.out.println("创建表成功");
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println("创建表失败，请检查");
        }
    }

    //删除表
    public static void dropTable(Statement statement,String databaseName,String tableName) throws SQLException {
        try {

            String sql = "drop table if exists "+databaseName+"."+tableName;
            statement.execute(sql);
            System.out.println("删除表成功");
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println("删除表失败，请检查");
        }
    }

    // 查询数据库
    public static void showDatabases(Statement statement){
        String sql = "show databases";
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                System.out.println("查询到数据库："+ rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("查询数据库出错，请检查");
        }
    }

    // 查询表
    public static void showTables(Statement statement,String databaseName){
        String sql = "show tables";
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                System.out.println("查询到数据库"+databaseName+"下的表："+ rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("查询表出错，请检查");
        }
    }

    //查看表结构
    public static void descTable(Statement statement,String databaseName,String tableName) throws SQLException {
        statement.execute("use "+databaseName);
        String sql = "desc "+tableName;
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                System.out.println("字段名："+rs.getString(1)+",类型："+rs.getString(2));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("查询表结构出错，请检查");
        }
    }

    //增加字段
    public static void addColumns(Statement statement, String databaseName, String tableName, Map<String,String> columns) throws SQLException {
        statement.execute("use "+databaseName);
        try{
            for(Map.Entry<String, String> column : columns.entrySet()){
                String sql = "alter table " + tableName + " add columns ("+column.getKey()+" "+column.getValue()+")";
                System.out.println(sql);
                statement.execute(sql);
                System.out.println("字段:"+column.getKey()+"增添成功");
            }
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println("增加字段失败");
        }
    }

    //增加字段
    public static void changeColumns(Statement statement, String databaseName, String tableName, Map<String,String> columns) throws SQLException {
        statement.execute("use "+databaseName);
        try{
            for(Map.Entry<String, String> column : columns.entrySet()){
                String sql = "alter table " + tableName + " change  ("+column.getKey()+" "+column.getValue()+")";
                System.out.println(sql);
                statement.execute(sql);
                System.out.println("字段:"+column.getKey()+"增添成功");
            }
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println("修改字段失败");
        }
    }

    //读取excel，存入hive
    public static void readLocalFile(Statement statement, String databaseName, String tableName, String localFilePath) throws SQLException {
        statement.execute("use "+databaseName);
        try{

            String sql = "load data local inpath '"+localFilePath +"' into table "+tableName +" PARTITION (data ='2021-05-26')";
            System.out.println("sql = "+sql);
            statement.execute(sql);
            System.out.println("导入数据成功");
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println("导入数据失败");
        }
    }

    //查看前n条数据
    public static void selectTable(Statement statement, String databaseName, String tableName, int n) throws SQLException {
        statement.execute("use "+databaseName);
        try{

            String sql = "select * from "+tableName+" limit " + n;
            ResultSet result = statement.executeQuery(sql);
//            while (result.next()){
//                String name = result.getString("name");
//                String orderdate = result.getString("orderdate");
//                int cost = result.getInt("cost");
//                System.out.println(" 姓名："+name+" 日期："+orderdate+" 花费：" + cost);
//
//
//            }
            System.out.println("查询数据成功");
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println("查询数据失败");
        }
    }

//    创建表并指定表文件的存放路径

}
