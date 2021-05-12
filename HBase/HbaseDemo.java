package HBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class HbaseDemo {
    public static void main(String[] args) {
        try {
            String tableName = "USER";
//            createTable(tableName, new String[] { "information", "contact" ,"address"});
            User user = new User("001", "xiaoming", "123456", "man", "20",
                    "13355550021", "1232821@csdn.com","beijing");
            insertData(tableName, user);
            User user2 = new User("002", "xiaohong", "654321", "female", "18",
                    "18757912212", "214214@csdn.com","shanghai");
            insertData(tableName, user2);
            List<User> list = getAllData(tableName);
            System.out.println("--------------------插入两条数据后--------------------");
            for (User user3 : list){
                System.out.println(user3.toString());
            }
            System.out.println("--------------------获取原始数据-----------------------");
            getNoDealData(tableName);
            System.out.println("--------------------根据rowKey查询--------------------");
            User user4 = getDataByRowKey(tableName, "user-001");
            System.out.println(user4.toString());
            System.out.println("--------------------获取指定单条数据-------------------");
            String user_phone = getCellData(tableName, "user-001", "contact", "phone");
            System.out.println(user_phone);
            User user5 = new User("test-003", "xiaoguang", "789012", "man", "22",
                    "12312132214", "856832@csdn.com","nanjing");
            insertData(tableName, user5);
            List<User> list2 = getAllData(tableName);
            System.out.println("--------------------插入测试数据后--------------------");
            for (User user6 : list2){
                System.out.println(user6.toString());
            }
            deleteByRowKey(tableName, "user-test-003");
            List<User> list3 = getAllData(tableName);
            System.out.println("--------------------删除测试数据后--------------------");
            for (User user7 : list3){
                System.out.println(user7.toString());
            }
//            deleteTable(tableName);//删除表格

        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    //连接集群
    public static Connection initHbase() throws IOException {

        Configuration configuration = HBaseConfiguration.create();//创建HBase配置
        configuration.set("hbase.zookeeper.property.clientPort", "2181");//设置zookeeper端口
        configuration.set("hbase.zookeeper.quorum", "server1,server2,server3");//设置zookeeper
        //集群配置↓
        //configuration.set("hbase.zookeeper.quorum", "101.236.39.141,101.236.46.114,101.236.46.113");
        configuration.set("hbase.master", "server1:60000");
        return ConnectionFactory.createConnection(configuration);
    }

    //创建表
    public static void createTable(String tableNmae, String[] cols) throws IOException {

        TableName tableName = TableName.valueOf(tableNmae);
        System.out.println(tableName);
        Admin admin = initHbase().getAdmin();
        if (admin.tableExists(tableName)) {
            System.out.println("表已存在！");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);//表
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);//列簇
                hTableDescriptor.addFamily(hColumnDescriptor);//增加列簇
            }
            admin.createTable(hTableDescriptor);//创建表
        }
    }

    //插入数据
    public static void insertData(String tableName, User user) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        Put put = new Put(("user-" + user.getId()).getBytes());
        //参数：1.列族名  2.列名  3.值
        put.addColumn("information".getBytes(), "username".getBytes(), user.getUsername().getBytes()) ;
        put.addColumn("information".getBytes(), "age".getBytes(), user.getAge().getBytes()) ;
        put.addColumn("information".getBytes(), "gender".getBytes(), user.getGender().getBytes()) ;
        put.addColumn("contact".getBytes(), "phone".getBytes(), user.getPhone().getBytes());
        put.addColumn("contact".getBytes(), "email".getBytes(), user.getEmail().getBytes());
        put.addColumn("address".getBytes(), "city".getBytes(), user.getCity().getBytes());
        Table table = initHbase().getTable(tablename);
        table.put(put);
    }

    //获取原始数据
    public static void getNoDealData(String tableName){
        try {
            Table table= initHbase().getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner resutScanner = table.getScanner(scan);
            for(Result result: resutScanner){
                System.out.println("scan:  " + result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //根据rowKey进行查询
    public static User getDataByRowKey(String tableName, String rowKey) throws IOException {

        Table table = initHbase().getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        User user = new User();
        user.setId(rowKey);
        //先判断是否有此条数据
        if(!get.isCheckExistenceOnly()){
            Result result = table.get(get);
            for (Cell cell : result.rawCells()){
                String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());//字节转换
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                switch (colName){
                    case "username":
                        user.setUsername(value);
                        break;
                    case "age":
                        user.setAge(value);
                        break;
                    case "gender":
                        user.setGender(value);
                        break;
                    case "phone":
                        user.setPhone(value);
                        break;
                    case "email":
                        user.setEmail(value);
                        break;
                    case "city":
                        user.setCity(value);
                        break;
                    default:
                        System.out.println("传入值有误，请检查");
                        break;
                }
//                if(colName.equals("username")){
//                    user.setUsername(value);
//                }
//                if(colName.equals("age")){
//                    user.setAge(value);
//                }
//                if (colName.equals("gender")){
//                    user.setGender(value);
//                }
//                if (colName.equals("phone")){
//                    user.setPhone(value);
//                }
//                if (colName.equals("email")){
//                    user.setEmail(value);
//                }
//                if (colName.equals("city")){
//                    user.setCity(value);
//                }
            }
        }
        return user;
    }

    //查询指定单cell内容
    public static String getCellData(String tableName, String rowKey, String family, String col){

        try {
            Table table = initHbase().getTable(TableName.valueOf(tableName));
            String result = null;
            Get get = new Get(rowKey.getBytes());
            if(!get.isCheckExistenceOnly()){
                get.addColumn(Bytes.toBytes(family),Bytes.toBytes(col));
                Result res = table.get(get);
                byte[] resByte = res.getValue(Bytes.toBytes(family), Bytes.toBytes(col));
                return result = Bytes.toString(resByte);
            }else{
                return result = "查询结果不存在";
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "出现异常";
    }

    //查询指定表名中所有的数据
    public static List<User> getAllData(String tableName){

        Table table = null;
        List<User> list = new ArrayList<User>();
        try {
            table = initHbase().getTable(TableName.valueOf(tableName));
            ResultScanner results = table.getScanner(new Scan());
            User user = null;
            for (Result result : results){
                String id = new String(result.getRow());
                System.out.println("用户名:" + new String(result.getRow()));
                user = new User();
                for(Cell cell : result.rawCells()){
                    String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    //String family =  Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                    String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    user.setId(row);
                    if(colName.equals("username")){
                        user.setUsername(value);
                    }
                    if(colName.equals("age")){
                        user.setAge(value);
                    }
                    if (colName.equals("gender")){
                        user.setGender(value);
                    }
                    if (colName.equals("phone")){
                        user.setPhone(value);
                    }
                    if (colName.equals("email")){
                        user.setEmail(value);
                    }
                    if (colName.equals("city")){
                        user.setCity(value);
                    }
                }
                list.add(user);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    //删除指定cell数据
    public static void deleteByRowKey(String tableName, String rowKey) throws IOException {

        Table table = initHbase().getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //删除指定列
        //delete.addColumns(Bytes.toBytes("contact"), Bytes.toBytes("email"));
        table.delete(delete);
    }

    //删除表
    public static void deleteTable(String tableName){

        try {
            TableName tablename = TableName.valueOf(tableName);
            Admin admin = initHbase().getAdmin();
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
class User {
    private String id;
    private String username;
    private String password;
    private String gender;
    private String age;
    private String phone;
    private String email;
    private String city;



    public User(String id, String username, String password, String gender, String age, String phone, String email,String city) {
        this.id = id;
        this.username = username;
        this.password = password;
        this.gender = gender;
        this.age = age;
        this.phone = phone;
        this.email = email;
        this.city = city;
    }


    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public User(){

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    @Override
    public String toString() {
        return "User{" +
                "id='" + id + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", gender='" + gender + '\'' +
                ", age='" + age + '\'' +
                ", phone='" + phone + '\'' +
                ", email='" + email + '\'' +
                ", city='" + city +'}';
    }


}
