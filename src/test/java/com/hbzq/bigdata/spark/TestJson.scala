package com.hbzq.bigdata.spark

import com.hbzq.bigdata.spark.domain.{BaseRecord, SlowMessageRecord, TsscjRecord}
import com.hbzq.bigdata.spark.utils.{JsonUtilV2}
import org.junit.Test

/**
  * describe:
  * create on 2020/07/22
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
class TestJson {
  @Test
  def testParseJsonTime: Unit = {
    val record: String =
      """
        |{"actseq":"212.213056","after":{"BPGDH":"A116015771","BRCJJE":"0","BRCJSL":"0","BZ":"RMB","BZS1":"0","CDSL":"0","CJBS":"0","CJJE":"0","CJJG":"0","CJSJ":"00:00:00","CJSL":"0","CXBZ":"O","CXWTH":"0","CZZD":"","DDJYXZ":"0","DDLX":"0","DDSXXZ":"0","DDYXRQ":"0","DJZJ":"537","FHGY":null,"FSYYB":"9999","GDH":"A116015771","GSFL":"00","ISIN":null,"JGSM":"待申报","JSJG":"GSYH","JSLX":"1","JSZH":"01000003305201","JYS":"SH","JYSDDBH":"0","KHH":"010000034067","KHQZ":"4A","KHXM":"XXX","LSH_ZJDJXM":"323090325","LSH_ZQDJXM":"0","PLWTPCH":"213","QSZJ":"0","SBGY":null,"SBJB":"0","SBJG":"0","SBJLH":"0","SBRQ":"0","SBSJ":"00:00:00","SBWTH":" ","SBXW":"20359","SQCZRH":null,"STEP":null,"TDBH":"1","WTFS":"32","WTGY":"90009103","WTH":"111118","WTJG":"5.32","WTLB":"1","WTRQ":"20200717","WTSJ":"14:50:58","WTSL":"100","XWZBH":"1","YWSQH":"0","YYB":"0100","ZDPLSL":"0","ZHGLJG":"0100","ZQDM":"600006","ZQLB":"A0","ZQMC":"东风汽车","ZSXJ":"0"},"after_key":{"WTH":"111118"},"jstime":"1594968495344076","name":"TDRWT","op":0,"optype":"INSERT","owner":"SECURITIES","scn":"9568559471","scntime":1594968658,"xid":"0x0007.029.00013c99"}
      """.stripMargin

    println("===========================")
    for (i <- 1 to 20) {
      val start = System.currentTimeMillis()
      for (i <- 1 to 1000) {

        JsonUtilV2.parseAllJsonStringToMap(record)
      }
      val end = System.currentTimeMillis()
      println(
        s"""
           |time use : ${end - start}
      """.stripMargin)
    }

    println("===========================")
    for (i <- 1 to 20) {
      val start = System.currentTimeMillis()
      for (i <- 1 to 1000) {

        //        JsonUtil.parseAllJsonStringToMap(record)
      }
      val end = System.currentTimeMillis()
      println(
        s"""
           |time use : ${end - start}
      """.stripMargin)
    }
  }

  @Test
  def testParseObject(): Unit = {
    val record = TsscjRecord("123",
      "123",
      "123",
      "123",
      BigDecimal(0),
      BigDecimal(0),
      "123",
      "123")
    record.version = record.version +1
    println(JsonUtilV2.parseObjectToJson( SlowMessageRecord("ffff",record)))



  }

}
