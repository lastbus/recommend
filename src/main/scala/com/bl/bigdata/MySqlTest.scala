package com.bl.bigdata

import java.io.File
import java.net.{URI, URISyntaxException, URL, URLClassLoader}
import java.util.Enumeration

import scala.collection.JavaConverters._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.util.{MutableURLClassLoader, ParentClassLoader, Utils}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK33 on 2016/11/8.
  */
object MySqlTest {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    hiveContext.sql("show tables").show()
    val loader = new MutableURLClassLoader(new Array[URL](0), Thread.currentThread().getContextClassLoader)
//    val l = Thread.currentThread().getContextClassLoader
    addJarToClasspath("/opt/cloudera/parcels/CDH-5.4.8-1.cdh5.4.8.p0.4/jars/mysql-connector-java-5.1.38.jar", loader)
    Thread.currentThread().setContextClassLoader(loader)
//    Class.forName("com.mysql.jdbc.Driver")
    hiveContext.load("jdbc", Map("url" -> "jdbc:mysql://10.201.129.74:3306/recommend_system?user=root&password=bl.com", "dbtable" -> "bl_category_performance_basic"))
//    val a = hiveContext.jdbc("jdbc:mysql://10.201.129.74:3306/recommend_system?user=root&password=bl.com", "bl_category_performance_basic")
    .take(10).foreach(println)




  }

  private def addJarToClasspath(localJar: String, loader: MutableURLClassLoader) {
    val uri = resolveURI(localJar)
    uri.getScheme match {
      case "file" | "local" =>
        val file = new File(uri.getPath)
        if (file.exists()) {
          loader.addURL(file.toURI.toURL)
        } else {
          println(s"Local jar $file does not exist, skipping.")
        }
      case _ =>
        println(s"Skip remote jar $uri.")
    }
  }

  def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (uri.getFragment() != null) {
        val absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI()
        return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      }
    } catch {
      case e: URISyntaxException =>
    }
    new File(path).getAbsoluteFile().toURI()
  }
}

/**
  * URL class loader that exposes the `addURL` and `getURLs` methods in URLClassLoader.
  */
class MutableURLClassLoader(urls: Array[URL], parent: ClassLoader)
  extends URLClassLoader(urls, parent) {

  override def addURL(url: URL): Unit = {
    super.addURL(url)
  }

  override def getURLs(): Array[URL] = {
    super.getURLs()
  }

}

/**
  * A mutable class loader that gives preference to its own URLs over the parent class loader
  * when loading classes and resources.
  */
class ChildFirstURLClassLoader(urls: Array[URL], parent: ClassLoader)
  extends MutableURLClassLoader(urls, null) {

  private val parentClassLoader = new ParentClassLoader(parent)

  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    try {
      super.loadClass(name, resolve)
    } catch {
      case e: ClassNotFoundException =>
        parentClassLoader.loadClass(name, resolve)
    }
  }

  override def getResource(name: String): URL = {
    val url = super.findResource(name)
    val res = if (url != null) url else parentClassLoader.getResource(name)
    res
  }

  override def getResources(name: String): Enumeration[URL] = {
    val childUrls = super.findResources(name).asScala
    val parentUrls = parentClassLoader.getResources(name).asScala
    (childUrls ++ parentUrls).asJavaEnumeration
  }

  override def addURL(url: URL) {
    super.addURL(url)
  }

}

class ParentClassLoader(parent: ClassLoader) extends ClassLoader(parent) {

  override def findClass(name: String): Class[_] = {
    super.findClass(name)
  }

  override def loadClass(name: String): Class[_] = {
    super.loadClass(name)
  }

  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    super.loadClass(name, resolve)
  }

}

