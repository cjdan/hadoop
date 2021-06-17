package Spark.SQL;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/*
 * 动态创建Schema的方式创建DataFrame
 */
public class CreateDataSetFromRDDWithStruct {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CreateDataSetFromRDDWithStruct");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);
        JavaRDD<String> textFile = jsc.textFile("file:///E:/data/rddfile.txt");
        /*
         * 装换成Row类型的RDD
         */
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

        /*
         * 动态构造DataFrame的元数据，一般而言，有多少列以及每列的具体类型可能来自于Json，也可能来自于DB
         */
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("gender", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

        /*
         * 构建StructType，用于最后DataFrame元数据的描述
         */
        StructType structType = DataTypes.createStructType(fields);
        /*
         * 基于已有的MetaData以及RDD<Row> 来构造Dataset
         */
        Dataset<Row> df = sqlContext.createDataFrame(map, structType);
        df.show();
        df.registerTempTable("structtable");

        Dataset<Row> result = sqlContext.sql("select * from structtable where age < 30");
        result.show();
        /*
         * 将结果转化成javaRDD foreach输出
         */
        result.javaRDD().foreach(new VoidFunction<Row>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            public void call(Row t) {
                System.out.println(t);
            }
        });

    }
}
