package com.hbzq.bigdata.spark.utils

import com.google.common.base.Preconditions
import com.hbzq.bigdata.spark.domain.BaseRecord

/**
  * describe:
  * create on 2020/05/27
  *
  * 规则校验
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
object RuleVaildUtil {
  var rules: Map[String, Map[String, List[Map[String, List[String]]]]] = _
  var inited: Boolean = false

  def init(rules: Map[String, Map[String, List[Map[String, List[String]]]]]): Unit = {
    this.rules = rules
    this.inited = true
  }



  /**
    *
    * 获取指定规则下的  分类列表
    *
    * @param ruleName
    * @return
    */
  def getClassifyListByRuleName(ruleName: String): Map[String, List[Map[String, List[String]]]] = {
    this.rules.get(ruleName).get
  }

  /**
    * 验证规则 由对象自己实现验证规则
    *
    * @param t
    * @param ruleName
    * @return
    */
  def matchClassify(t: BaseRecord, ruleName: String): String = {
    Preconditions.checkArgument(inited, "rules not be inited,please load rules before vaild...", None)
    val classifyRule = getClassifyListByRuleName(ruleName)
    t.matchClassify(classifyRule)
  }

  /**
    * 根据规则名+分类名  获取到该分类下的所有匹配规则
    *
    * @param ruleName
    * @param classifyName
    * @return
    */
  def getRuleListByName(ruleName: String, classifyName: String): List[Map[String, List[String]]] = {
    this.rules.get(ruleName).get(classifyName)
  }

}
