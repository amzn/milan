package com.amazon.milan.application.metrics

import com.amazon.milan.application.MetricDefinition
import com.amazon.milan.program.FunctionDef
import com.amazon.milan.program.internal.ConvertExpressionHost
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import org.apache.commons.lang.builder.HashCodeBuilder

import scala.language.experimental.macros
import scala.reflect.macros.whitebox


/**
 * Definition of a Histogram metric
 *
 * @param valueFunction  The definition of the function that extracts the metric value from the input record.
 * @param name           The name of the histogram metric.
 * @param reservoirSize  The number of samples to keep in the sampling reservoir.
 * @param reservoirAlpha The exponential decay factor; the higher this is, the more biased the reservoir
 *                       will be towards newer values.
 */
@JsonSerialize
@JsonDeserialize
class HistogramDefinition[T: TypeDescriptor](val valueFunction: FunctionDef,
                                             val name: String,
                                             val reservoirSize: Int = 1028,
                                             val reservoirAlpha: Double = 0.015D) extends MetricDefinition[T] {
  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(obj: Any): Boolean = obj match {
    case o: HistogramDefinition[T] =>
      this.valueFunction.equals(o.valueFunction) &&
        this.name == o.name &&
        this.reservoirSize == o.reservoirSize &&
        this.reservoirAlpha == o.reservoirAlpha

    case _ =>
      false
  }
}


object HistogramDefinition {
  /**
   * Create a [[HistogramDefinition]] using the specified value extractor function.
   *
   * @param valueFunction  A function that extracts the metric value from input records.
   * @param name           The name of the histogram metric.
   * @param reservoirSize  The number of samples to keep in the sampling reservoir.
   * @param reservoirAlpha The exponential decay factor; the higher this is, the more biased the reservoir
   *                       will be towards newer values.
   * @tparam T The input record type.
   * @return A [[HistogramDefinition]] containing the histogram metric.
   */
  def create[T](valueFunction: T => Long,
                name: String,
                reservoirSize: Int,
                reservoirAlpha: Double): HistogramDefinition[T] = macro HistogramDefinitionMacros.create[T]

  /**
   * Create a [[HistogramDefinition]] using the specified value extractor function and the default reservoir
   * parameters.
   *
   * @param valueFunction A function that extracts the metric value from input records.
   * @param name          The name of the histogram metric.
   * @tparam T The input record type.
   * @return A [[HistogramDefinition]] containing the histogram metric.
   */
  def create[T](valueFunction: T => Long,
                name: String): HistogramDefinition[T] = macro HistogramDefinitionMacros.createDefaultReservoir[T]
}


class HistogramDefinitionMacros(val c: whitebox.Context) extends ConvertExpressionHost {

  import c.universe._

  def createDefaultReservoir[T: c.WeakTypeTag](valueFunction: c.Expr[T => Long],
                                               name: c.Expr[String]): c.Expr[HistogramDefinition[T]] = {
    this.create(valueFunction, name, c.Expr[Int](q"1028"), c.Expr[Double](q"0.015D"))
  }

  def create[T: c.WeakTypeTag](valueFunction: c.Expr[T => Long],
                               name: c.Expr[String],
                               reservoirSize: c.Expr[Int],
                               reservoirAlpha: c.Expr[Double]): c.Expr[HistogramDefinition[T]] = {
    val valueFunctionDef = this.getMilanFunction(valueFunction.tree)
    val tree = q"new ${weakTypeOf[HistogramDefinition[T]]}($valueFunctionDef, $name, $reservoirSize, $reservoirAlpha)"
    c.Expr[HistogramDefinition[T]](tree)
  }
}
