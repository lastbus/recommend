package com.bl.bigdata

import java.io.{BufferedReader, File, FileReader}
import java.lang.reflect.Method
import java.util

import com.bl.bigdata.datasource.HBaseConnectionFactory
import net.sf.cglib.proxy.{Enhancer, MethodInterceptor, MethodProxy}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{Appender, AppenderSkeleton, Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ArrayBuffer
import scala.collection.script.Message
import scala.collection.{LinearSeq, SeqLike, mutable}
import scala.collection.JavaConversions._

/**
  * Created by MK33 on 2016/10/24.
  */
object GG {
  var stackLength = 0

  def main(args: Array[String]) {
//    methodOOM

    println("HH")


  }

  def methodOOM = {
    while (true) {
      val enhancer = new Enhancer
      enhancer.setSuperclass(classOf[StackTest])
      enhancer.setUseCache(false)
      enhancer.setCallback(new MethodInterceptor {
        override def intercept(o: scala.Any, method: Method, objects: Array[AnyRef], methodProxy: MethodProxy): AnyRef = {
          methodProxy.invoke(o, objects)
        }
      })
      enhancer.create()
    }
  }

  // VM args: -XX:PermSize=10M -XX:MaxPermSize=10M
  def runtimeConstantPool = {
    val array = new ArrayBuffer[String]()
    var i = 0L
    while (true) {
      array.append(i.toString.intern())
      i += 1
    }
  }

  // VM args: -Xss128k
  def outOfStack: Unit = {
    stackLength += 1
    GG.outOfStack
  }

  //vm args -verbose -XX:+PrintGCDetails -Xms20m -Xmx20M -XX:+HeapDumpOnOutOfMemoryError
  def outOfMemory = {
    val arrayBuf = new ArrayBuffer[Rational]()
    while (true) {
      arrayBuf.append(new Rational(2, 3))
    }
  }

  def scan = {
    val logger = Logger.getRootLogger
    var rpcCount = 0
    val appender = new AppenderSkeleton() {
      override def append(loggingEvent: LoggingEvent): Unit = {
        val msg = loggingEvent.getMessage.toString
        println(msg)
        if (msg != null) {
          rpcCount += 1
        }
      }

      override def requiresLayout(): Boolean = {
        false
      }

      override def close(): Unit = {}
    }

    logger.removeAllAppenders()
    logger.setAdditivity(false)
    logger.addAppender(appender)
    logger.setLevel(Level.DEBUG)

    // 影响操作效率：每次RPC取回的条数
    val scan = new Scan
    scan.setStartRow(Bytes.toBytes("2016-11-03 08:44:19"))
    scan.setStopRow(Bytes.toBytes("2016-11-03 08:46:09"))
    scan.setCaching(100000)
    println(scan.getCaching)
    println(scan.setBatch(1000))
    println(scan.getBatch)
    println(scan.getCacheBlocks)
    scan.getAttributesMap.foreach(s => println(s._1 + "\t" + Bytes.toString(s._2)))
    scan.addFamily(Bytes.toBytes("info"))
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("member_longitude"))
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("member_latitude"))

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "m79sit,s80sit,s81sit")
    val conn = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(TableName.valueOf("real_time_lat_lng"))

    val start = System.currentTimeMillis()
    val resultSet = table.getScanner(scan)
    val list = new java.util.ArrayList[(String, String)](2)
    var i = 0
    for (rs <- resultSet) { // 每得到一行记录都是单独的 RPC 请求，一次要取多行数据则要打开扫描器缓存，默认它是关闭的
      val longitude = rs.getValue(Bytes.toBytes("info"), Bytes.toBytes("member_longitude"))
      val latitude = rs.getValue(Bytes.toBytes("info"), Bytes.toBytes("member_latitude"))
      list.add((Bytes.toString(latitude), Bytes.toString(longitude)))
      i += 1
    }


    //scanner 会占用服务器端的堆空间，用完一定要关闭
    resultSet.close()
    conn.close()

    println("There are " + i + " records.")
    val end = System.currentTimeMillis()
    println("Time taken is " + (end - start) / 1000)
    println("PRC count: " + rpcCount)

  }





}


class StackTest {
  var stackLength = 0
  def stackLeak: Unit = {
    stackLength += 1
    stackLeak
  }

  def stackThread = {
    while (true) {
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          while (true){}
        }
      })
      thread.start()
    }
  }
}

class Animal {

  def p = {
    println("Animal")
  }

}
trait Furry extends Animal {

  override def p = {
    println("Furry")
    super.p
  }

}
trait HasLegs extends Animal {

  override def p = {
    println("Has Legs")
    super.p
  }

}
trait FourLegged extends HasLegs {

  override def p = {
    println("Four Legged")
    super.p
  }

}
class Cat extends Animal with Furry with FourLegged {

  override def p = {
    println("cat")
    super.p
  }

}


class T extends A {

  override def print(s: String) = {
    println(" == TT ===")
  }

}

trait A {

  def print(s: String)

}

trait AA extends A {

  abstract override def print(s: String) = {
    println("AA")
    super.print(s)
  }

}

trait BB extends A {

  abstract override def print(s: String) = {
    println("BB")
    super.print(s)
  }

}

abstract class IntQueue {
  def get(): Int
  def put(x: Int)
}
class BasicIntQueue extends IntQueue {
  private val buf = new ArrayBuffer[Int]
  def get() = buf.remove(0)
  def put(x: Int) = { buf += x }
}

class Rational(val n: Int, val d: Int) extends Ordered[Rational] {
  require(d != 0, "除数不能为0")
  override def compare(that: Rational): Int = {
    this.n * that.d - this.d * that.n
  }

}


object QuickSortBetterTypes {
  def sort[T, Coll](a: Coll)(implicit ev0: Coll <:< SeqLike[T, Coll],
                             cbf: CanBuildFrom[Coll, T, Coll],
                             n: Ordering[T]): Coll = {
    if (a.size < 2) a
    else {
      import n._
      val pivot = a.head
      val (lower: Coll, tmp: Coll) = a.partition(_ < pivot)
      val (upper: Coll, same: Coll) = tmp.partition(_ > pivot)
      val b = cbf()
      b.sizeHint(a.length)
      b ++= sort[T, Coll](lower)
      b ++= same
      b ++= sort[T, Coll](upper)
      b.result()
    }
  }
}

object NaiveQuickSort {
  def sort[T](a: Iterable[T])(implicit n: Ordering[T]): Iterable[T] = {
    if (a.size < 2) a
    else {
      import n._
      val pivot = a.head
      sort(a.filter((_ < pivot))) ++
      a.filter(_ == pivot) ++
      sort(a.filter(_ > pivot))
    }
  }
}

object x extends ArrayBuffer[Int] with mutable.ObservableBuffer[Int] {
  subscribe(new Sub{
    override def notify(pub: Pub,
                        evt: Message[Int] with mutable.Undoable) = {
      Console println ("Event: " + evt + " from " + pub)
    }
  })
}

class FileLineTraversable(file: File) extends Traversable[String] {
  /** 内部迭代器 */
  override def foreach[U](f: String => U): Unit = {
    println("opening file: " + file.getAbsolutePath)
    val input = new BufferedReader(new FileReader(file))
    try {
      var line = input.readLine()
      while (line != null) {
        f(line)
        line = input.readLine()
      }
      println("Done iterating file")
    } finally {
      println("Closing file")
      input.close()
    }
  }

  override def toString = {
    "{Line of }" + file.getAbsolutePath + "}"
  }
}

sealed trait BinaryTree[+A] {

  def traverse[A, U](t: BinaryTree[A])(f: A => U): Unit = {
    @annotation.tailrec
    def traverseHelper(current: BinaryTree[A],
                       next: LinearSeq[BinaryTree[A]]): Unit = {
      current match {
        case Branch(value, lhs, rhs) =>
          f(value)
          traverseHelper(lhs, rhs +: next)
        case Leaf(value) if !next.isEmpty =>
          f(value)
          traverseHelper(next.head, next.tail)
        case Leaf(value) => f(value)
        case NilTree if !next.isEmpty =>
          traverseHelper(next.head, next.tail)
        case NilTree => ()
      }
    }
    traverseHelper(t, LinearSeq())
  }

}
case object NilTree extends BinaryTree[Nothing]
case class Branch[+A](value: A,
                      lhs: BinaryTree[A],
                      rhs: BinaryTree[A]) extends BinaryTree[A]
case class Leaf[+A](value: A) extends BinaryTree[A]



