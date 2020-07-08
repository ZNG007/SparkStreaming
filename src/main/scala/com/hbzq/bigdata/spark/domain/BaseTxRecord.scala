package com.hbzq.bigdata.spark.domain

import scala.reflect.runtime.universe._
import scala.util.control.Breaks

/**
  * describe:
  * create on 2020/05/27
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
trait BaseTxRecord {


  /**
    * 根据规则分类列表   获取记录所属规则分类
    *
    * @param classifys
    * @param _type
    * @return
    */
  def matchClassify(classifys: Map[String, List[Map[String, List[String]]]], _type: Type): String = {
    var classify: String = "qt"
    for ((classify, rules) <- classifys) {
      for (rule <- rules) {
        var flag: Boolean = false
        val loop = new Breaks;
        loop.breakable {
          for ((fieldName, limitList) <- rule) {
            val field = getFieldValueByName(fieldName.toLowerCase, _type);
            if (limitList.contains(field)) {
              flag = true
            } else {
              flag = false
              loop.break()
            }
          }
        }
        if (flag) return classify
      }
    }
    classify
  }

  /**
    * 反射机制  根据字段名称获取字段值
    *
    * @param name
    * @param _type
    * @return
    */
  def getFieldValueByName(name: String, _type: Type): String = {
    val mirror = runtimeMirror(getClass().getClassLoader)
    val instanceMirror = mirror.reflect(this)
    val nameField = _type.decl(TermName(name)).asTerm
    instanceMirror.reflectField(nameField).get.toString
  }
}

// TDRWT   "KHH","WTH","YYB","WTFS", "WTGY","BZ","WTSL","WTJG"
case class TdrwtRecord(var khh: String, var wth: String
                       , var yyb: String, var wtfs: String, var wtgy: String,
                       var bz: String, var wtsl: Int, var wtjg: BigDecimal,
                       var channel: String = "qt") extends BaseTxRecord




