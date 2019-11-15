package com.amazon.milan.test

import java.time.Instant

import com.amazon.milan.Id
import com.amazon.milan.types.Record


object IntRecord {
  def apply(i: Int): IntRecord = new IntRecord(i)
}

class IntRecord(val recordId: String, val i: Int) extends Record with Comparable[IntRecord] {
  def this(i: Int) {
    this(Id.newId(), i)
  }

  override def getRecordId: String = this.recordId

  override def compareTo(o: IntRecord): Int = this.i.compareTo(o.i)

  override def equals(obj: Any): Boolean = obj match {
    case o: IntRecord => this.i == o.i
    case _ => false
  }
}

object StringRecord {
  def apply(s: String): StringRecord = new StringRecord(s)
}

class StringRecord(val recordId: String, val s: String) extends Record {
  def this(s: String) {
    this(Id.newId(), s)
  }

  override def getRecordId: String = this.recordId

  override def equals(obj: Any): Boolean = obj match {
    case o: StringRecord => this.s == o.s
    case _ => false
  }
}

object KeyValueRecord {
  def apply(key: String, value: String): KeyValueRecord = new KeyValueRecord(key, value)
}

// We need this to be recognized as a POJO class to use it in a keyBy operation in Flink.
class KeyValueRecord(var recordId: String, var key: String, var value: String) extends Record {
  def this() {
    this(Id.newId(), "", "")
  }

  def this(key: String, value: String) {
    this(Id.newId(), key, value)
  }

  override def getRecordId: String = this.recordId

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: KeyValueRecord => this.key == o.key && this.value == o.value
      case _ => false
    }
  }
}

object TwoIntRecord {
  def apply(a: Int, b: Int): TwoIntRecord = new TwoIntRecord(a, b)
}

class TwoIntRecord(val recordId: String, val a: Int, val b: Int) extends Record {
  def this(a: Int, b: Int) {
    this(Id.newId(), a, b)
  }

  override def getRecordId: String = this.recordId

  override def equals(obj: Any): Boolean = obj match {
    case o: TwoIntRecord => this.a == o.a && this.b == o.b
    case _ => false
  }
}

object NumbersRecord {
  def apply(i1: Int, i2: Int, d1: Double, d2: Double, f1: Float): NumbersRecord =
    new NumbersRecord(Id.newId(), i1, i2, d1, d2, f1)
}

class NumbersRecord(val recordId: String, val i1: Int, val i2: Int, val d1: Double, val d2: Double, val f1: Float)
  extends Record {

  override def getRecordId: String = this.recordId
}


object IntKeyValueRecord {
  def apply(key: Int, value: Int): IntKeyValueRecord = new IntKeyValueRecord(key, value)
}

class IntKeyValueRecord(var recordId: String, var key: Int, var value: Int) extends Record {
  def this() {
    this(Id.newId(), 0, 0)
  }

  def this(key: Int, value: Int) {
    this(Id.newId(), key, value)
  }

  override def getRecordId: String = this.recordId

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: IntKeyValueRecord => this.key == o.key && this.value == o.value
      case _ => false
    }
  }
}

object IntStringRecord {
  def apply(i: Int, s: String): IntStringRecord = new IntStringRecord(i, s)
}

class IntStringRecord(val recordId: String, val i: Int, val s: String) extends Record {
  def this(i: Int, s: String) {
    this(Id.newId(), i, s)
  }

  override def getRecordId: String = this.recordId

  override def equals(obj: Any): Boolean = obj match {
    case o: IntStringRecord => this.i == o.i && this.s == o.s
    case _ => false
  }
}


object DateIntRecord {
  def apply(dateTime: Instant, i: Int): DateIntRecord = new DateIntRecord(dateTime, i)
}

class DateIntRecord(val recordId: String, val dateTime: Instant, val i: Int) extends Record {
  def this(dateTime: Instant, i: Int) {
    this(Id.newId(), dateTime, i)
  }

  def this(dateTime: Instant) {
    this(Id.newId(), dateTime, 0)
  }

  override def getRecordId: String = this.recordId

  override def equals(obj: Any): Boolean = obj match {
    case o: DateIntRecord => this.dateTime.equals(o.dateTime) && this.i == o.i
    case _ => false
  }
}


object DateKeyValueRecord {
  def apply(dateTime: Instant, key: Int, value: Int): DateKeyValueRecord = new DateKeyValueRecord(Id.newId(), dateTime, key, value)
}

class DateKeyValueRecord(val recordId: String, val dateTime: Instant, val key: Int, val value: Int) extends Record {
  override def getRecordId: String = this.recordId

  override def equals(obj: Any): Boolean = obj match {
    case o: DateKeyValueRecord => this.dateTime.equals(o.dateTime) && this.key == o.key && this.value == o.value
    case _ => false
  }
}


object Tuple3Record {
  def apply[T1, T2, T3](f0: T1, f1: T2, f2: T3): Tuple3Record[T1, T2, T3] = new Tuple3Record[T1, T2, T3](f0, f1, f2)
}

class Tuple3Record[T1, T2, T3](var recordId: String, var f0: T1, var f1: T2, var f2: T3) extends Record {
  def this(f0: T1, f1: T2, f2: T3) {
    this(Id.newId(), f0, f1, f2)
  }

  override def getRecordId: String = this.recordId

  override def equals(obj: Any): Boolean = obj match {
    case o: Tuple3Record[T1, T2, T3] => this.f0.equals(o.f0) && this.f1.equals(o.f1) && this.f2.equals(o.f2)
    case _ => false
  }
}


object Tuple4Record {
  def apply[T1, T2, T3, T4](f0: T1, f1: T2, f2: T3, f3: T4): Tuple4Record[T1, T2, T3, T4] = new Tuple4Record[T1, T2, T3, T4](f0, f1, f2, f3)
}

class Tuple4Record[T1, T2, T3, T4](var recordId: String, var f0: T1, var f1: T2, var f2: T3, var f3: T4) extends Record {
  def this(f0: T1, f1: T2, f2: T3, f3: T4) {
    this(Id.newId(), f0, f1, f2, f3)
  }

  override def getRecordId: String = this.recordId

  override def equals(obj: Any): Boolean = obj match {
    case o: Tuple4Record[T1, T2, T3, T4] => this.f0.equals(o.f0) && this.f1.equals(o.f1) && this.f2.equals(o.f2) && this.f3.equals(o.f3)
    case _ => false
  }
}