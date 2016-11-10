package com.srisunt.spark;

import com.srisunt.hbase.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ssrisunt on 10/30/16.
 */
public class SparkHbase {


    private final static String ttableName = "TEST";
    private final static String tcolumnFamily = "cf";

    public static void main(String[] args) throws IOException {
        String tableName = null;
        String columnFamily = null;
        if (args.length < 2) {
            System.out.println("JavaHBaseBulkPutExample  " +
                    "{tableName} {columnFamily}");
            tableName=ttableName;
            columnFamily=tcolumnFamily;
        }

        Configuration conf = HbaseUtils.loginKeyTab(args);


        JavaSparkContext jsc = new JavaSparkContext("local[*]","Spark Hive");

        try {
            List<String> list = new ArrayList<>();
            list.add("1," + columnFamily + ",a,1");
            list.add("2," + columnFamily + ",a,2");
            list.add("3," + columnFamily + ",a,3");
            list.add("4," + columnFamily + ",a,4");
            list.add("5," + columnFamily + ",a,5");

            JavaRDD<String> rdd = jsc.parallelize(list);

            // new Hadoop API configuration
            Job newAPIJobConfiguration = Job.getInstance(conf);
            newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
            newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

            writeRowNewHadoopAPI(rdd,newAPIJobConfiguration.getConfiguration(),columnFamily);

            saveToHBase(rdd, newAPIJobConfiguration.getConfiguration());

        } finally {
            jsc.stop();
        }
    }



    public static class PutFunction implements Function<String, Put> {

        private static final long serialVersionUID = 1L;

        public Put call(String v) throws Exception {
            String[] cells = v.split(",");
            Put put = new Put(Bytes.toBytes(cells[0]));

            put.add(Bytes.toBytes(cells[1]), Bytes.toBytes(cells[2]),
                    Bytes.toBytes(cells[3]));
            return put;
        }

    }

    //6. saveToHBase method - insert data into HBase
    public static void saveToHBase(JavaRDD<String> javaRDD, Configuration conf) throws IOException
    {
        // create Key, Value pair to store in HBase
        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = javaRDD.mapToPair(
                new PairFunction<String, ImmutableBytesWritable, Put>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, Put> call(String v) throws Exception {

                        String[] cells = v.split(",");
                        Put put = new Put(Bytes.toBytes(cells[0]));

                        put.addColumn(Bytes.toBytes(cells[1]), Bytes.toBytes(cells[2]),
                                Bytes.toBytes(cells[3]));

                        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
                    }
                });

        // save to HBase- Spark built-in API method
        hbasePuts.saveAsNewAPIHadoopDataset(conf);
    }

    private static void writeRowNewHadoopAPI(JavaRDD<String> records,Configuration conf, String columnFamily) {


        //FIXME: mirar como quitar la carga de un texto arbitrario para crear un JavaRDD
        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = records.mapToPair(new PairFunction<String, ImmutableBytesWritable, Put>() {
            @Override
            public Tuple2<ImmutableBytesWritable, Put> call(String t)
                    throws Exception {
                Put put = new Put(Bytes.toBytes("rowkey11"));
                put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("a"),
                        Bytes.toBytes("value1"));
                put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("b"),
                        Bytes.toBytes("value2"));
                put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("z"),
                        Bytes.toBytes("value3"));

                return new Tuple2<ImmutableBytesWritable, Put>(
                        new ImmutableBytesWritable(), put);
            }
        });

        hbasePuts.saveAsNewAPIHadoopDataset(conf);
        //sparkContext.stop();

    }


}
