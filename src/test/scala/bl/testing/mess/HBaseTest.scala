package bl.testing.mess

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK33 on 2016/3/8.
  */
object HBaseTest {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("test-hbase")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
//    val conf = sc.hadoopConfiguration
    conf.set(TableOutputFormat.OUTPUT_TABLE, "test_make")
    val job = Job.getInstance(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[Put]])

    val put = new Put(Bytes.toBytes("a"))
    val put2 = new Put(Bytes.toBytes("b"))
    put.addColumn(Bytes.toBytes("column_family1"), Bytes.toBytes("test"), Bytes.toBytes("1"))
    put2.addColumn(Bytes.toBytes("column_family1"), Bytes.toBytes("test"), Bytes.toBytes("1"))
    val puts = Array(put, put2)
    val localData = Array(("a", args(0)), ("b", args(1)))
    println(puts.length)

    val r = sc.parallelize(localData).
      map(s =>{
        val put = new Put(Bytes.toBytes(s._1))
        put.addColumn(Bytes.toBytes("column_family1"), Bytes.toBytes("int"), Bytes.toBytes(s._2))
      }).
      map(put => (new ImmutableBytesWritable("key".getBytes()), put))

    r.saveAsNewAPIHadoopDataset(job.getConfiguration)

    sc.stop()

  }

}
