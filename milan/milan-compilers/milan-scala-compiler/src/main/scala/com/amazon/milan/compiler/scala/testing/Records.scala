package com.amazon.milan.compiler.scala.testing

import com.amazon.milan.Id

import scala.util.Random


class IntRecord(val recordId: String, val i: Int) {
  override def toString: String = s"IntRecord($i)"

  override def equals(obj: Any): Boolean = obj match {
    case o: IntRecord => this.i == o.i
    case _ => false
  }
}

object IntRecord {
  def apply(i: Int): IntRecord = new IntRecord(Id.newId(), i)
}


class KeyValueRecord(val recordId: String, val key: Int, val value: Int) {
  override def toString: String = s"($key: $value)"

  override def equals(obj: Any): Boolean = obj match {
    case o: KeyValueRecord => this.key == o.key && this.value == o.value
    case _ => false
  }
}

object KeyValueRecord {
  def apply(key: Int, value: Int): KeyValueRecord = new KeyValueRecord(Id.newId(), key, value)

  def generate(recordCount: Int, maxKey: Int, maxValue: Int): List[KeyValueRecord] = {
    val rand = new Random()
    List.tabulate(recordCount)(_ => KeyValueRecord(rand.nextInt(maxKey), rand.nextInt(maxValue)))
  }
}


object TwoIntRecord {
  def apply(a: Int, b: Int): TwoIntRecord = new TwoIntRecord(a, b)
}

class TwoIntRecord(val recordId: String, val a: Int, val b: Int) {
  def this(a: Int, b: Int) {
    this(Id.newId(), a, b)
  }

  override def toString: String = s"(${this.a}, ${this.b})"

  override def equals(obj: Any): Boolean = obj match {
    case o: TwoIntRecord => this.a == o.a && this.b == o.b
    case _ => false
  }
}

object ThreeIntRecord {
  def apply(a: Int, b: Int, c: Int): ThreeIntRecord = new ThreeIntRecord(a, b, c)
}

class ThreeIntRecord(val recordId: String, val a: Int, val b: Int, val c: Int) {
  def this(a: Int, b: Int, c: Int) {
    this(Id.newId(), a, b, c)
  }

  override def toString: String = s"(${this.a}, ${this.b}, ${this.c})"

  override def equals(obj: Any): Boolean = obj match {
    case o: ThreeIntRecord => this.a == o.a && this.b == o.b && this.c == o.c
    case _ => false
  }
}


object FourIntRecord {
  def apply(a: Int, b: Int, c: Int, d: Int): FourIntRecord = new FourIntRecord(a, b, c, d)
}

class FourIntRecord(val recordId: String, val a: Int, val b: Int, val c: Int, val d: Int) {
  def this(a: Int, b: Int, c: Int, d: Int) {
    this(Id.newId(), a, b, c, d)
  }

  override def toString: String = s"(${this.a}, ${this.b}, ${this.c}, ${this.d})"

  override def equals(obj: Any): Boolean = obj match {
    case o: FourIntRecord => this.a == o.a && this.b == o.b && this.c == o.c && this.d == o.d
    case _ => false
  }
}
