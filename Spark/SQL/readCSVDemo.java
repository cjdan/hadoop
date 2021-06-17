package Spark.SQL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class readCSVDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CreateDataFrameFromList");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        List<StructField> names = new ArrayList<>();
        names.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        names.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        names.add(DataTypes.createStructField("gender", DataTypes.StringType, true));
        names.add(DataTypes.createStructField("height", DataTypes.IntegerType, true));
        names.add(DataTypes.createStructField("weight", DataTypes.IntegerType, true));
        names.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        JavaRDD<String> people = jsc.textFile("file:///E:/data/people.csv");
        JavaRDD<Row> map = people.map(new Function<String, Row>() {
            private static final long serialVersionUID = 1L;

            public Row call(String s) {
                String[] split = s.split(",");
                //id,name,gender,height,weight,age
                return RowFactory.create(Integer.valueOf(split[0]),split[1],split[2],Integer.valueOf(split[3]),Integer.valueOf(split[4]),Integer.valueOf(split[5]));
            }
        });
        StructType structType = DataTypes.createStructType(names);
        Dataset<Row> df = sqlContext.createDataFrame(map, structType);
//        df.filter(df.col("id"))
        df.registerTempTable("people");
        Dataset<Row> result = sqlContext.sql("select * from people where age = 16");

        df.show();

    }
}
