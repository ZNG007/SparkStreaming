package com.hbzq.bigdata.spark

import com.hbzq.bigdata.spark.domain.TdrwtRecord
import com.hbzq.bigdata.spark.operator.rdd.TkhxxOperator
import com.hbzq.bigdata.spark.utils.{DateUtil, HBaseUtil, JsonUtil}

import scala.util.Random

/**
  * describe:
  * create on 2020/07/20
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
object Test {
  def main(args: Array[String]): Unit = {
    println(HBaseUtil.getRowKeyFromInteger(111118
    ))
    /*val a :Object= TdrwtRecord(
      "",
      "xxxx",
      "nnn",
      "",
      "",
      "",
      "",
      0,
      BigDecimal(0)
    )
    a match {
      case obj: TdrwtRecord =>  print(obj.wth)
    }*/


  }

}
