package com.hbzq.bigdata.spark.operator.rdd

trait RddOperator extends Serializable{
  def compute() : Any
}
