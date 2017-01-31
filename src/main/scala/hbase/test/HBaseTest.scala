// ======================================================================
//
//      Copyright (C) 北京国双科技有限公司
//                    http://www.gridsum.com
//
//      保密性声明：此文件属北京国双科技有限公司所有，仅限拥有由国双科技
//      授予了相应权限的人所查看和所修改。如果你没有被国双科技授予相应的
//      权限而得到此文件，请删除此文件。未得国双科技同意，不得查看、修改、
//      散播此文件。
//
//
// ======================================================================

package hbase.test

import java.nio.ByteBuffer

import hbase.test.utils.Util
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object HBaseTest extends Util {

  /**
    * java -jar interfacetest-1.0-jar-with-dependencies.jar /home/wangxiaoxi/tempDatas/ wxx-test-100
    * "testMethod=BatchByFile&addCount=500&batchSize=100&testInterfaces=JNI,THRIFT"
    * @param args
    */
  def main(args: Array[String]): Unit = {
    var tempPath = "D:\\project\\hbase-test\\target\\tempDatas"
    var tableName = "wxx-test-100"
    if (args != null && args.length >= 2) {
      tempPath = args(0)
      tableName = args(1)
    }

    var addCount = 500
    var batchSize = 100
    var testMethod = ""
    var interfaces = Array("JNI", "THRIFT")
    var src = ""
    var dest = ""
    if (args != null && args.length >= 3) {
      val params = args(2).split("&")
      params.foreach(param => {
        val kv = param.split("=")
        val value = kv(1)
        kv(0) match {
          case "testInterfaces" => interfaces = value.split(",")
          case "testMethod" => testMethod = value
          case "addCount" => addCount = value.toInt
          case "batchSize" => batchSize = value.toInt
          case "src" => src = value
          case "dest" => dest = value
        }
      })
    }

    log.info(
      s"""
    var tempPath = $tempPath
    var tableName = $tableName
    var addCount = $addCount
    var batchSize = $batchSize
    var testMethod = $testMethod
    var interfaces = $interfaces""")

    val templates = new mutable.HashMap[String, HBaseDaoTestTemplate]
    for (interface <- interfaces) {
      interface match {
        case "JNI" =>
          //"gs-server-v-129,gs-server-v-128,gs-server-v-127"
          templates(interface) = HBaseDaoTestTemplate(InterfaceType.JNI, "gs-server-1028,gs-server-1032,gs-server-1031,gs-server-1027,gs-server-1033")
        case "REST" =>
          templates(interface) = HBaseDaoTestTemplate(InterfaceType.REST, "10.200.200.56", 20550)
        case "THRIFT" =>
          //"10.200.200.56"
          templates(interface) = HBaseDaoTestTemplate(InterfaceType.THRIFT, "gs-server-1028", 9090)
        case _ => throw new IllegalArgumentException(s"illegal argument interface:$interface")
      }
    }

    for (templateEntry <- templates) {
      val template = templateEntry._2
      val templateName = templateEntry._1

      testMethod match {
        case "BatchByFile" => template.testBatchByFile(tableName, 0 until addCount, tempPath, batchSize)
        case "PutByFile" => template.testPutByFile(tableName, 0 until addCount, tempPath)
        case "ReadFromFile2Compact" => readFromFile2Compact(src, dest, "\t", addCount)
      }

      //    template.testGet(RowKey(tableName, "rk0001"))
      //    template.testGetValue(GetKey(tableName, "rk0002", "cf1", "name"))
      //    template.testPut(RowKey(tableName, "rk0009"))
      //      template.testScan(RowKeyRange(tableName, "0000400000", "0000400002"))
      //      template.testBatch(Seq(RowKey(tableName, "rk0014"), RowKey(tableName, "rk0015")))
      // 初始化数据，向HBase中插入数据，每2000条执行一次批处理，数据格式为（每行3个列族，每个列族60列，随机插入45列）
      //    template.initDatas(tableName, 200001 to 1000000, 3, 45, 60, 2000)
      // 在0-100000行数据中，每一行的指定列族中都插入一列数据，逐条插入
      //      template.testAddColumnDatas(tableName, 0 to 100000, "cf3", templateName)
      // 在0-100000行数据中，每一行的指定列族中都插入一列数据，每2000行执行一次批量操作
      //      template.testAddColumnDatasByBatch(tableName, 0 to 100000, "cf3", templateName, 2000)
      // 通过Scan批量查询一批数据，并修改每一个列族的每一列数据，并提交批量修改数据，每1000行执行一次批量操作,修改10W行数据
      //      template.testUpdateColumnDatasByBatch(tableName, Range(400000, 500000, 1000), templateName)
      // 通过getRow在100W的表中随机查询一条数据，并修改每一个列族的每一列数据，并提交修改，共修改1W行数据。
      //      template.testUpdateColumnDatas(tableName, 10000, 1000000, templateName)
      // 通过getRow在100W的表中随机查询一条数据，并修改每一个列族的每一列数据，并批量提交修改，每1000条批量写入一次，共修改1W条数据。
      //      template.testUpdateColumnDatasByWriteBatch(tableName, 10000, 1000000, templateName, 1000)
      // 通过Scan批量查询一批数据，并修改每一个列族的每一列数据，并逐条提交修改数据，每1000行执行一次顺序查询操作，共修改1W行数据
      //      template.testUpdateColumnDatasByScan(tableName, Range(50000, 60000, 500), templateName)

      // 通过获取分页数据，由于数据太多，又不知道RowKey时使用（只有JavaNativeAPI方式实现了该方法）
      //      template.testSaveOneRowByFilter(tableName, "tempDatas")
      // 通过文件获取存在磁盘上的信息，并打印出来
      //      template.testGetByFile(tableName, "tempDatas")
    }
  }

}
