package com.bl.bigdata

import java.io.{FileReader, BufferedReader, File}



import scala.collection.LinearSeq

/**
 * Created by MK33 on 2016/5/29.
 */
object study {

  def main(args: Array[String]) {

    (1 to 100) filter (2 to 3) .toSet
    val binaryTree = Branch(1, Leaf(2, Branch(3, Leaf(4), NilTree)), NilTree)
    binaryTree.traverse(binaryTree)(s => println(s"000 + $s"))

    val address = Map(2 -> 2).withDefaultValue("0")


  }

  sealed trait BinaryTree[+A]
  case object NilTree extends BinaryTree[Nothing]
  case class Branch[+A](value: A,
                        lhs: BinaryTree[A],
                        rhs: BinaryTree[A]) extends BinaryTree[A] {

    def traverse[A, U](t: BinaryTree[A])(f: A=>U): Unit = {
      //    @annotation.tailrec
      def traverseHelper(current: BinaryTree[A],
                         next: LinearSeq[BinaryTree[A]]): Unit ={
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

        traverseHelper(t, LinearSeq())
      }
    }
  }
  case class Leaf[+A](value: A) extends BinaryTree[A]

}




class TT (file: File)extends Traversable[String] {
  override def foreach[U](f: String => U): Unit = {
    println(s"file ${file} was opened")
    val input = new BufferedReader(new FileReader(file))
    try {
      var line = input.readLine()
      while (line != null)
      {
        f(line)
        line = input.readLine()
      }
      println("done iterating file")
    } finally {
      println("closed file")
      input.close()
    }
  }

  override def toString() = {
    s"{lines of ${file.getAbsolutePath} }"
  }
}


