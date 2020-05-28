package com.hbzq.bigdata.spark.domain

import scala.util.control.Breaks

/**
  * describe:
  * create on 2020/05/27
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
trait BaseRecord {

  /**
    * 根据规则分类列表   获取记录所属规则分类
    *
    * @param classifys
    * @return
    */
  def matchClassify(classifys: Map[String, List[Map[String, List[String]]]]): String = {
    var classify: String = ""
    for ((classify, rules) <- classifys) {
      for (rule <- rules) {
        var flag: Boolean = false
        val loop = new Breaks;
        loop.breakable {
          for ((fieldName, limitList) <- rule) {
            val field = getFieldValueByName(fieldName.toLowerCase);
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

  val ru = scala.reflect.runtime.universe
  val mirror = ru.runtimeMirror(getClass().getClassLoader)

  /**
    * 反射机制  根据字段名称获取字段值
    *
    * @param name
    * @return
    */
  def getFieldValueByName(name: String): String = {

    val instanceMirror = mirror.reflect(this)
    val nameField = ru.typeOf[TdrwtRecord].decl(ru.TermName(name)).asTerm
    instanceMirror.reflectField(nameField).get.toString
  }
}

// TDRWT   "KHH","WTH","YYB","WTFS", "WTGY", "CJJE","BZ","CXWTH","CLASSIFY"
case class TdrwtRecord(var khh: String, var wth: String
                       ,var yyb:String,var wtfs:String,var wtgy: String,
                       var cjje:String,var bz:String,var cxwth:String,var channel:String="其他") extends BaseRecord
