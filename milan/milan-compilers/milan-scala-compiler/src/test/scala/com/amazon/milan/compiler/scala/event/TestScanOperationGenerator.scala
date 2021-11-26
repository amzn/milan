package com.amazon.milan.compiler.scala.event

import com.amazon.milan.compiler.scala.event.operators.ScanOperation
import com.amazon.milan.compiler.scala.testing.IntRecord
import com.amazon.milan.compiler.scala.{DefaultTypeEmitter, RuntimeEvaluator, TypeLifter}
import com.amazon.milan.lang._
import com.amazon.milan.program.{ScanExpression, Tree, TypeChecker}
import com.amazon.milan.typeutil.types
import org.junit.Assert._
import org.junit.Test


object TestScanOperationGenerator {
  def addInts(t: (IntRecord, IntRecord)): IntRecord = {
    val (x, y) = t
    IntRecord(x.i + y.i)
  }
}


@Test
class TestScanOperationGenerator {
  @Test
  def test_ScanOperationGenerator_GenerateCombinedScanOperations_WithOneMaxOperation_HasSameBehaviorAsMaxOperation(): Unit = {
    val outputs = new GeneratorOutputs(new DefaultTypeEmitter)

    val input = Stream.of[IntRecord]
    val maxBy = input.maxBy(r => r.i)

    val inputStream = StreamInfo(maxBy.expr, types.EmptyTuple, types.Int)

    val streamTypes = Map(input.streamId -> input.expr.streamType)
    TypeChecker.typeCheck(maxBy.expr, streamTypes)

    val generator = new ScanOperationGenerator {
      override val typeLifter: TypeLifter = new TypeLifter()
    }

    val innerOpInfo = generator.generateScanOperationClass(outputs, inputStream, maxBy.expr.asInstanceOf[ScanExpression])

    val outputFunctionDef = Tree.fromFunction((key: Int, t: Tuple1[IntRecord]) => t match {
      case Tuple1(r) => r
    })
    TypeChecker.typeCheck(outputFunctionDef)

    val combinedScanOp = generator.generateCombinedScanOperations(outputs, inputStream, List(innerOpInfo), outputFunctionDef)

    val createInstanceCode = s"new ${combinedScanOp.classDef}"
    val scanOp = RuntimeEvaluator.default.eval[ScanOperation[IntRecord, Int, Tuple1[Option[Int]], IntRecord]](createInstanceCode)

    val (state1, output1) = scanOp.process(Tuple1(None), IntRecord(1), 0)
    assertEquals(IntRecord(1), output1)

    val (state2, output2) = scanOp.process(state1, IntRecord(3), 0)
    assertEquals(IntRecord(3), output2)

    // If we pass in something less thaRn the maximum then we shouldn't get any output.
    val (state3, output3) = scanOp.process(state2, IntRecord(2), 0)
    assertEquals(IntRecord(3), output3)
    assertEquals(Tuple1(Some(3, IntRecord(3))), state3)
  }

  @Test
  def test_ScanOperationGenerator_GenerateCombinedScanOperations_WithMaxAndSumOperations_HasExpectedOutput(): Unit = {
    val outputs = new GeneratorOutputs(new DefaultTypeEmitter)

    val input = Stream.of[IntRecord]
    val maxBy = input.maxBy(r => r.i)
    val sumBy = input.sumBy(r => r.i, (r, s) => IntRecord(s))

    val inputStream = StreamInfo(maxBy.expr, types.EmptyTuple, types.Int)

    val streamTypes = Map(input.streamId -> input.expr.streamType)
    TypeChecker.typeCheck(maxBy.expr, streamTypes)
    TypeChecker.typeCheck(sumBy.expr, streamTypes)

    val generator = new ScanOperationGenerator {
      override val typeLifter: TypeLifter = new TypeLifter()
    }

    val maxInfo = generator.generateScanOperationClass(outputs, inputStream, maxBy.expr.asInstanceOf[ScanExpression])
    val sumInfo = generator.generateScanOperationClass(outputs, inputStream, sumBy.expr.asInstanceOf[ScanExpression])

    val outputFunctionDef = Tree.fromFunction((key: Int, t: (IntRecord, IntRecord)) => TestScanOperationGenerator.addInts(t))
    TypeChecker.typeCheck(outputFunctionDef)

    val combinedScanOp = generator.generateCombinedScanOperations(outputs, inputStream, List(maxInfo, sumInfo), outputFunctionDef)

    val createInstanceCode = s"new ${combinedScanOp.classDef}"

    val scanOp = RuntimeEvaluator.default.eval[ScanOperation[IntRecord, Int, (Option[Int], Int), IntRecord]](createInstanceCode)

    val (state1, output1) = scanOp.process((None, 0), IntRecord(1), 0)
    assertEquals(IntRecord(2), output1)

    val (state2, output2) = scanOp.process(state1, IntRecord(3), 0)
    assertEquals(IntRecord(7), output2)

    // If we pass in something less than the maximum then we shouldn't get any output.
    val (state3, output3) = scanOp.process(state2, IntRecord(2), 0)
    assertEquals(IntRecord(9), output3)
    assertEquals((Some(3, IntRecord(3)), 6), state3)
  }
}
