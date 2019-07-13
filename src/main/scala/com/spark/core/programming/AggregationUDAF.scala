package com.spark.core.programming

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class AggregationUDAF extends UserDefinedAggregateFunction {

  private def aggregate(value1: String, value2: String): String = {
    var valArr1 = value1.split(";").map(_.toInt)
    var valArr2 = value2.split(";").map(_.toInt)

    val difference = math.abs(valArr1.length - valArr2.length)
    val filledArr = Array.fill(difference)(0)

    if (valArr1.length < valArr2.length)
      valArr1 = valArr1 ++ filledArr
    else
      valArr2 = valArr2 ++ filledArr

    valArr1.zip(valArr2).map(t => t._1 + t._2).mkString(";")
  }

  // input field fo aggregation
  override def inputSchema: StructType = {
    StructType(StructField("value", StringType) :: Nil)
  }

  // internal fields to keep for computing aggregation
  override def bufferSchema: StructType = {
    StructType(StructField("aggregate", StringType) :: Nil)
  }

  // output type of our aggregation function
  override def dataType: DataType = {
    StringType
  }


  override def deterministic: Boolean = {
    true
  }

  // initial values for buffer schema
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = "0"
  }

  // updating the buffer schema given an input
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = aggregate(buffer.getAs[String](0), input.getAs[String](0))
  }

  // merging two objects with the bufferSchema type
  override def merge(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = aggregate(buffer.getAs[String](0), input.getAs[String](0))
  }

  //outputting the final value
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[String](0)
  }

}
