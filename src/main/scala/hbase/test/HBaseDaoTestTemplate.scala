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

import hbase.test.HBaseTest.readFromFile
import hbase.test.InterfaceType.InterfaceType
import hbase.test.impl.{JNIHBaseDao, RestfulHBaseDao, ThriftHBaseDao}
import hbase.test.utils.{TimedUtil, Util}
import org.apache.hadoop.hbase.util.Bytes
import sun.util.resources.el.CalendarData_el

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class HBaseDaoTestTemplate(hBaseDao: HBaseDao) extends Util with TimedUtil {

  private def getStrValue(qualifier: String, value: Array[Byte]): String = {
    val strValue = if ("age".equals(qualifier)) {
      ByteBuffer.wrap(value).getInt.toString
    } else {
      Bytes.toString(value)
    }
    strValue
  }

  def testGetValue(getKey: GetKey): Unit = {
    openAndInit(getKey.tableName)
    val result = hBaseDao.get(getKey)
    //'wxx-hbase.test', 'rk0001', 'cf1:name', 'changkun'
    result match {
      case Some(r) =>
        println(getStrValue(getKey.qualifier, r))
      case _ => println("null")
    }
  }

  private def openAndInit(tableName: String) = {
    hBaseDao.open()
    hBaseDao.init(Map("tableName" -> tableName))
  }

  private def printRow(cells: Seq[CellValue]): Unit = {
    for (cell <- cells) {
      val value = getStrValue(cell.qualifier, cell.value)
      println(s"CellValue: ${cell.columnFamily},${cell.qualifier},$value,${cell.timestamp}")
    }
  }

  def testGet(rowKey: RowKey): Unit = {
    openAndInit(rowKey.tableName)
    val list = hBaseDao.getRow(rowKey)
    printRow(list)
    //'wxx-hbase.test', 'rk0001', 'cf1:name', 'wangxiaoxi'
  }

  def testScan(rowKeyRange: RowKeyRange): Unit = {
    openAndInit(rowKeyRange.tableName)
    val rows = hBaseDao.scan(rowKeyRange)
    for (row <- rows) {
      println(s"RowKey: ${row._1}")
      printRow(row._2)
    }
  }

  /**
    * 插入数据测试
    *
    * @param rowKey 行标识
    */
  def testPut(rowKey: RowKey): Unit = {
    openAndInit(rowKey.tableName)
    val result = hBaseDao.put(rowKey, Seq(
      CellValue("cf1", "name", bytes("changkun"), 0),
      CellValue("cf1", "age", Bytes.toBytes(24), 0),
      CellValue("cf2", "addr", bytes("chengdu"), 0),
      CellValue("cf2", "sex", bytes("female"), 0)))
    println(result)
  }

  /**
    * 批处理测试
    *
    * @param keys 需要插入的Rowkey数组
    */
  def testBatch(keys: Seq[RowKey]): Unit = {
    openAndInit(keys.head.tableName)
    val cellValues = Seq(
      CellValue("cf1", "name", bytes("changkun"), 0),
      CellValue("cf1", "age", Bytes.toBytes(24), 0),
      CellValue("cf2", "addr", bytes("chengdu"), 0),
      CellValue("cf2", "sex", bytes("female"), 0))
    var batch = ArrayBuffer[BatchCellValue]()
    for (elem <- keys) {
      batch += BatchCellValue(elem.rowKey, BatchType.ADD, cellValues)
    }
    hBaseDao.batch(keys.head.tableName, batch)
    println("Finish testBatch")
    hBaseDao.close()
  }

  private def addRandomColumnRow(columns: ArrayBuffer[CellValue], columnFamily: String, takeNum: Int, maxNum: Int): Unit = {
    val randomIndex = Random.shuffle((0 until maxNum).seq).take(takeNum)
    randomIndex.foreach(index => {
      columns += CellValue(columnFamily, "q-" + index, bytes("v-" + index), 0)
    })
  }

  /**
    * 初始化数据
    *
    * @param tableName   表名
    * @param rowKeyRange 插入rowKey范围
    * @param cfNum       列族个数
    * @param takeNum     插入的列数
    * @param maxNum      拥有的列数
    * @param batchSize   每次批量插入的数量
    */
  def initDatas(tableName: String, rowKeyRange: Range, cfNum: Int, takeNum: Int, maxNum: Int, batchSize: Int): Unit = {
    openAndInit(tableName)
    log.info("started initDatas.")
    val putBatch = new ArrayBuffer[BatchCellValue]
    val usedTime = timed {
      for (id <- rowKeyRange) {
        val rowkeyStr = "%010d".format(id)
        val put = new ArrayBuffer[CellValue]
        for (cfIndex <- 1 to cfNum) {
          addRandomColumnRow(put, "cf" + cfIndex, takeNum, maxNum)
        }
        putBatch += BatchCellValue(rowkeyStr, BatchType.ADD, put)
        if (id % batchSize == 0) {
          hBaseDao.batch(tableName, putBatch)
          putBatch.clear()
          log.info(s"finished $id recorders.")
        }
      }
      if (putBatch.nonEmpty) {
        hBaseDao.batch(tableName, putBatch)
      }
    }
    log.info(s"finished initDatas.(usedTime:$usedTime)")
    hBaseDao.close()
  }

  /**
    * 逐条插入一列值(即个制定的rowkey范围内，都会插入这个列，而不是随机插入)
    *
    * @param tableName   表名
    * @param rowKeyRange 插入rowKey范围
    * @param cf          列族名
    * @param qualifier   列名
    */
  def testAddColumnDatas(tableName: String, rowKeyRange: Range, cf: String, qualifier: String): Unit = {
    openAndInit(tableName)
    val functionName = "testAddColumnDatas"
    val interfaceName = hBaseDao.getClass.getSimpleName
    log.info(s"started $functionName.( Interface:$interfaceName )")
    val usedTime = timed {
      for (id <- rowKeyRange) {
        val rowkeyStr = "%010d".format(id)
        // 每行的指定列族的制定一个列都放入一个值
        hBaseDao.put(RowKey(tableName, rowkeyStr), Seq(CellValue(cf, qualifier, bytes(s"v-$qualifier-$id"), 0)))
        if (id % 2000 == 0) {
          log.info(s"finished $id recorders.")
        }
      }
    }
    log.info(s"finished $functionName.(usedTime:$usedTime)")
    hBaseDao.close()
  }

  /**
    * 插入一列值(即指定的rowkey范围内，都会插入这个列，而不是随机插入)
    *
    * @param tableName   表名
    * @param rowKeyRange 插入rowKey范围
    * @param cf          列族名
    * @param qualifier   列名
    * @param batchSize   每次批量插入的数量
    */
  def testAddColumnDatasByBatch(tableName: String, rowKeyRange: Range, cf: String, qualifier: String, batchSize: Int): Unit = {
    openAndInit(tableName)
    val functionName = "testAddColumnDatasByBatch"
    val interfaceName = hBaseDao.getClass.getSimpleName
    log.info(s"started $functionName.( Interface:$interfaceName )")
    val putBatch = new ArrayBuffer[BatchCellValue]
    val usedTime = timed {
      for (id <- rowKeyRange) {
        val rowkey = "%010d".format(id)
        val put = new ArrayBuffer[CellValue]
        // 每行的指定列族的一个列都放入一个值
        put += CellValue(cf, qualifier, bytes(s"v-$qualifier-$id"), 0)
        putBatch += BatchCellValue(rowkey, BatchType.ADD, put)

        if (id % batchSize == 0) {
          hBaseDao.batch(tableName, putBatch)
          putBatch.clear()
          log.info(s"finished $id recorders.")
        }
      }
      if (putBatch.nonEmpty) {
        hBaseDao.batch(tableName, putBatch)
      }
    }
    log.info(s"finished $functionName.(usedTime:$usedTime)")
    hBaseDao.close()
  }

  /**
    * 随机读逐条写数据
    *
    * @param tableName HBase表名
    * @param count     随机逐条更新的数量
    * @param maxRowKey 随机获取数据的最大Rowkey值
    * @param suffix    对每列修改的值（由于更新操作是在原来的值上，添加后缀，所以这个参数就是添加的后缀）
    */
  def testUpdateColumnDatas(tableName: String, count: Int, maxRowKey: Int, suffix: String): Unit = {
    openAndInit(tableName)
    val functionName = "testUpdateColumnDatas"
    val interfaceName = hBaseDao.getClass.getSimpleName
    log.info(s"started $functionName.( Interface:$interfaceName )")
    val usedTime = timed {
      // 分批遍历数据
      for (id <- 0 to count) {
        // 首先随机取出一条数据
        var rowKey = RowKey(tableName, "%010d".format(Random.nextInt(maxRowKey)))
        val getResult = hBaseDao.getRow(rowKey)

        val newValues: ArrayBuffer[CellValue] = updateCellValues(suffix, getResult)
        //进行数据修改
        hBaseDao.put(rowKey, newValues)
        if (id % 1000 == 0)
          log.info(s"finished $id recorders.")
      }
    }
    log.info(s"finished $functionName.(usedTime:$usedTime)")
    hBaseDao.close()
  }

  /**
    * 顺序读逐条写数据
    *
    * @param tableName   HBase表名
    * @param rowKeyRange 批量更新的RowKey范围
    * @param suffix      对每列修改的值（由于更新操作是在原来的值上，添加后缀，所以这个参数就是添加的后缀）
    */
  def testUpdateColumnDatasByScan(tableName: String, rowKeyRange: Range, suffix: String): Unit = {
    openAndInit(tableName)
    val functionName = "testUpdateColumnDatasByBatch"
    val interfaceName = hBaseDao.getClass.getSimpleName
    log.info(s"started $functionName.( Interface:$interfaceName )")
    val usedTime = timed {
      // 分批遍历数据
      for (id <- rowKeyRange) {
        // 首先取出数据
        val batchLastId = math.min(rowKeyRange.last, id + rowKeyRange.step)
        val batchRowKeyRange = RowKeyRange(tableName, "%010d".format(id), "%010d".format(batchLastId))
        val scanResult = hBaseDao.scan(batchRowKeyRange)

        //逐条进行数据修改
        for (rowResult <- scanResult) {
          val rowKey = RowKey(tableName, rowResult._1)
          val put: ArrayBuffer[CellValue] = updateCellValues(s"-$suffix-U4", rowResult._2)
          hBaseDao.put(rowKey, put)
        }
        log.info(s"finished $id recorders.")
      }
    }
    log.info(s"finished $functionName.(usedTime:$usedTime)")
    hBaseDao.close()
  }

  /**
    * 随机读批量写数据
    *
    * @param tableName HBase表名
    * @param count     随机逐条更新的数量
    * @param maxRowKey 随机获取数据的最大Rowkey值
    * @param suffix    对每列修改的值（由于更新操作是在原来的值上，添加后缀，所以这个参数就是添加的后缀）
    * @param batchSize 批处理数量
    */
  def testUpdateColumnDatasByWriteBatch(tableName: String, count: Int, maxRowKey: Int, suffix: String, batchSize: Int): Unit = {
    openAndInit(tableName)
    val functionName = "testUpdateColumnDatas"
    val interfaceName = hBaseDao.getClass.getSimpleName
    log.info(s"started $functionName.( Interface:$interfaceName )")
    val usedTime = timed {
      // 分批遍历数据
      for (id <- 0 to count) {
        // 首先随机取出一条数据
        var rowKey = RowKey(tableName, "%010d".format(Random.nextInt(maxRowKey)))
        val getResult = hBaseDao.getRow(rowKey)
        val batchResult = mutable.Map[String, Seq[CellValue]]()
        // 存入带批处理的Map中
        batchResult(rowKey.rowKey) = getResult
        if (id % batchSize == 0) {
          val updatedDatas = updateBatchCellValues(s"-$suffix-U3", batchResult)
          hBaseDao.batch(tableName, updatedDatas)
          log.info("Random rowKey:" + rowKey)
          log.info(s"finished $id recorders.")
        }
      }
    }
    log.info(s"finished $functionName.(usedTime:$usedTime)")
    hBaseDao.close()
  }

  /**
    * 顺序读批量写数据
    *
    * @param tableName   HBase表名
    * @param rowKeyRange 批量更新的RowKey范围
    * @param suffix      对每列修改的值（由于更新操作是在原来的值上，添加后缀，所以这个参数就是添加的后缀）
    */
  def testUpdateColumnDatasByBatch(tableName: String, rowKeyRange: Range, suffix: String): Unit = {
    openAndInit(tableName)
    val functionName = "testUpdateColumnDatasByBatch"
    val interfaceName = hBaseDao.getClass.getSimpleName
    log.info(s"started $functionName.( Interface:$interfaceName )")
    val usedTime = timed {
      // 分批遍历数据
      for (id <- rowKeyRange) {
        // 首先取出数据
        val batchLastId = math.min(rowKeyRange.last, id + rowKeyRange.step)
        val batchRowKeyRange = RowKeyRange(tableName, "%010d".format(id), "%010d".format(batchLastId))
        val scanResult = hBaseDao.scan(batchRowKeyRange)

        //批量进行数据修改
        val putBatch: ArrayBuffer[BatchCellValue] = updateBatchCellValues(s"-$suffix-U1", scanResult)
        hBaseDao.batch(tableName, putBatch)
        log.info(s"finished $id recorders.")
      }
    }
    log.info(s"finished $functionName.(usedTime:$usedTime)")
    hBaseDao.close()
  }

  /**
    * 修改传入的数据，每个列族每个列都加上指定的后缀
    *
    * @param suffix    后缀
    * @param getResult 需要修改的数据
    * @return
    */
  private def updateCellValues(suffix: String, getResult: Seq[CellValue]) = {
    // 把取出的数据插入HBase
    val newValues = ArrayBuffer[CellValue]()
    for (oldCell <- getResult) {
      val newValue = string(oldCell.value) + suffix
      val newCell = CellValue(oldCell.columnFamily, oldCell.qualifier, bytes(newValue), 0)
      newValues += newCell
    }
    newValues
  }

  /**
    * 批量修改传入的数据，每个列族每个列都加上指定的后缀
    *
    * @param suffix     后缀
    * @param scanResult 需要修改的数据
    * @return
    */
  private def updateBatchCellValues(suffix: String, scanResult: mutable.Map[String, Seq[CellValue]]) = {
    // 遍历取到的数据，并修改每个列的值，在原有的基础上加上"suffix"的后缀
    val putBatch = ArrayBuffer[BatchCellValue]()
    for (old <- scanResult) {
      val rowkeyStr = old._1
      val newValues = ArrayBuffer[CellValue]()
      for (oldCell <- old._2) {
        val newValue = string(oldCell.value) + suffix
        val newCell = CellValue(oldCell.columnFamily, oldCell.qualifier, bytes(newValue), 0)
        newValues += newCell
      }
      putBatch += BatchCellValue(rowkeyStr, BatchType.ADD, newValues)
    }
    putBatch
  }

  /**
    * 获取分页数据，由于数据太多，又不知道RowKey时使用
    * （只有JavaNativeAPI方式实现了该方法）
    *
    * @param tableName 表名
    */
  def testSaveOneRowByFilter(tableName: String, tempPath: String): Unit = {
    openAndInit(tableName)
    val rows = hBaseDao.scanByPageFilter(tableName)
    var i = 0
    for (row <- rows if i == 0) {
      i += 1
      println(s"RowKey: ${row._1}")
      saveAsFile(tempPath, row._1, row._2)
    }
  }

  def testGetByFile(tableName: String, tempPath: String): Unit = {
    val (key, datas) = readFromFile(tempPath)
    openAndInit(tableName)
    val list = hBaseDao.getRow(RowByteKey(tableName, key))
    printRow(list)
    //'wxx-hbase.test', 'rk0001', 'cf1:name', 'wangxiaoxi'
  }

  def testPutByFile(tableName: String, range: Range, tempPath: String): Unit = {
    val functionName = "testPutByFile"
    val interfaceName = hBaseDao.getClass.getSimpleName

    // 通过读取文件，生成数据
    val (key, datas) = readFromFile(tempPath)

    openAndInit(tableName)
    log.info(s"started $functionName.( Interface:$interfaceName )")
    val usedTime = timed {
      for (index <- range) {
        val randomSuffix = "%010d".format(math.abs(Random.nextInt()))
        val key = s"""${"%03d".format(Random.nextInt(30))}_$randomSuffix"""
        log.debug("key=========" + key)
        hBaseDao.put(RowKey(tableName, key), datas)
        if (index % 2000 == 0) {
          log.info(s"finished $index recorders.")
        }
      }
    }
    log.info(s"Finish $functionName.(usedTime:$usedTime)")
    hBaseDao.close()
  }

  /**
    * 通过指定的文件进行批处理测试
    *
    * @param range 需要插入的Rowkey数组
    */
  def testBatchByFile(tableName: String, range: Range, tempPath: String, batchSize: Int): Unit = {
    val functionName = "testBatchByFile"
    val interfaceName = hBaseDao.getClass.getSimpleName

    // 通过读取文件，生成数据
    val (key, datas) = readFromFile(tempPath)

    openAndInit(tableName)
    log.info(s"started $functionName.( Interface:$interfaceName )")
    val usedTime = timed {
      var batch = ArrayBuffer[BatchCellValue]()
      for (index <- range) {
        val randomSuffix = "%010d".format(math.abs(Random.nextInt()))
        val key = s"""${"%03d".format(Random.nextInt(30))}_$randomSuffix"""
        println("key=========" + key)
        batch += BatchCellValue(key, BatchType.ADD, datas)
        if (index % batchSize == 0) {
          hBaseDao.batch(tableName, batch)
          batch.clear()
          log.info(s"finished $index recorders.")
        }
      }
      if (batch.nonEmpty)
        hBaseDao.batch(tableName, batch)
    }
    log.info(s"Finish $functionName.(usedTime:$usedTime)")
    hBaseDao.close()
  }

  def readDataFromPath(tempPath: String): Unit = {
    // 通过读取文件，生成数据
    readFromFile(tempPath)
  }
}

object HBaseDaoTestTemplate {

  def apply(hBaseDao: HBaseDao): HBaseDaoTestTemplate = new HBaseDaoTestTemplate(hBaseDao)

  def apply(interfaceType: InterfaceType, address: String = "", port: Int = 0): HBaseDaoTestTemplate = {

    val dao: HBaseDao = interfaceType match {
      case InterfaceType.JNI => JNIHBaseDao(address)
      case InterfaceType.REST => RestfulHBaseDao(address, port)
      case InterfaceType.THRIFT => ThriftHBaseDao(address, port)
    }
    new HBaseDaoTestTemplate(dao)
  }
}
