package Spark.SQL;


import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.SparkSession;


public class operateHiveDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .master("local[*]")
                //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .config("hadoop.home.dir", "/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();

//        spark.table("study.students").createOrReplaceTempView("students");
//        spark.sql("SELECT * FROM students where height > 176 order by id desc").show();
        //保存到hive表
//        //删除hive中的infos表
//        spark.sql("DROP TABLE IF EXISTS study.infos");
//        //删除hive中的infos表
//        spark.sql("DROP TABLE IF EXISTS infos");
        //在hive中创建infos表
//        spark.sql("CREATE TABLE IF NOT EXISTS study.infos (name STRING,age INT) "
//                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");
//        //创建好infos表后，给infos表导入数据
//        spark.sql("LOAD DATA LOCAL INPATH 'file:///E:/data/student_infos' INTO TABLE study.infos");

        //删除score表
//        spark.sql("DROP TABLE IF EXISTS study.score");
//        创建score表
//        spark.sql("CREATE TABLE IF NOT EXISTS study.score (name STRING,score INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");
//        //给score表导入数据
//        spark.sql("LOAD DATA LOCAL INPATH 'file:///E:/data/student_scores' INTO TABLE study.score");

        //读取hive 创建DataFrame
        Dataset<Row> resultDF = spark.sql("SELECT t1.name,t1.age,t2.score FROM study.infos t1 , study.score t2 "
                + "WHERE t1.name = t2.name AND t2.score > 60");
//        Column df1 = df.col("age");

        /*
        HIVE_STATS_JDBC_TIMEOUT：spark与hive冲突
        * */
//        df.show();
        resultDF.show();

    }
}
