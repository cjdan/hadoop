package Spark.SQL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class SparkSQLDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkSQLDemo");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        /*
         * 读取 json文件
         */
        Dataset<Row> json = sqlContext.read().json("file:///E:/data/scorejson.txt");
        System.out.println("=====");
        json.show();
        json.registerTempTable("score");

        /*
         * List<Person> 类型转换成RDD
         */
        List<Person> list = new ArrayList<>();
        list.add(new Person(1, "Zhang San", "m", 10));
        list.add(new Person(2, "Li Si", "f", 11));
        list.add(new Person(3, "Wang Wu", "m", 12));
        Dataset<Row> createDataFrame = sqlContext.createDataFrame(list, Person.class);
        Column column = createDataFrame.col("name").as("名字");
        System.out.println("column:"+column);

        System.out.println("*****");
        StructType schema = createDataFrame.schema();
        System.out.println("schema : "+schema);
        createDataFrame.show();
        createDataFrame.registerTempTable("person1");

        /*
         * structType 结构创建DataFrame
         */
        JavaRDD<String> textFile = jsc.textFile("file:///E:/data/rddfile.txt");
        JavaRDD<Row> map = textFile.map(new Function<String, Row>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            public Row call(String line) {
                String[] split = line.split(" ");
                return RowFactory.create(Integer.valueOf(split[0]),split[1],split[2],Integer.valueOf(split[3]));
            }
        });

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("gender", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

        StructType createStructType = DataTypes.createStructType(fields);

        Dataset<Row> createDataFrame2 = sqlContext.createDataFrame(map, createStructType);
        createDataFrame2.registerTempTable("person");

        List<String> collect = createDataFrame.javaRDD().map(new Function<Row, String>() {

            /*
             *
             */
            private static final long serialVersionUID = 1L;

            public String call(Row v1) {
                return v1.getAs("name");
            }
        }).collect();

        StringBuilder sql = new StringBuilder("select * from person t where t.name in (");
        for(String s : collect){
            sql.append("'").append(s).append("',");
        }
        sql = new StringBuilder(sql.substring(0, sql.length() - 1));
        sql.append(")");

        sqlContext.sql(sql.toString()).show();
    }
}
