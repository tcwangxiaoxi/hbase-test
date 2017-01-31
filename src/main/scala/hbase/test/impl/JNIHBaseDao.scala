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

package hbase.test.impl

import java.io.IOException
import java.util

import hbase.test._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.PageFilter
import org.apache.hadoop.hbase.util.Bytes.getBytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, HConstants, TableName}

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class JNIHBaseDao(zkQuorum: String) extends HBaseDao {

  private var conn: Connection = _

  private val tableMap = new mutable.HashMap[String, Table]()

  override def open(): Unit = {
    val cfg: Configuration = HBaseConfiguration.create()
    cfg.set("hbase.zookeeper.quorum", zkQuorum)
    conn = ConnectionFactory.createConnection(cfg)
  }

  /**
    * 初始化，如JNI方式的Table对象，可以在这里初始化，
    * 防止在操作调用时，算入操作的计算时间内
    *
    * @param params
    */
  override def init(params: Map[String, String]): Unit = {
    val tableName = params.get("tableName").get
    getTable(tableName)
  }

  private def getTable(tableName: String): Table = {
    var table = tableMap.get(tableName)
    if (table.isEmpty) {
      log.info(s"init table.(tableName:$tableName)")
      table = Some(conn.getTable(TableName.valueOf(tableName)))
      tableMap.put(tableName, table.get)
    }
    table.get
  }

  override def get(key: GetKey): Option[Array[Byte]] = {
    val userTable = getTable(key.tableName)
    val get = new Get(bytes(key.rowKey))
    val rs = userTable.get(get)
    val result = rs.getValue(bytes(key.columnFamily), bytes(key.qualifier))
    if (result == null) {
      return None
    }
    Some(result)
  }

  private def getCellValues(cells: util.List[Cell]): Seq[CellValue] = {
    val result = new ListBuffer[CellValue]
    cells.asScala.foreach {
      cell =>
        val bValue = CellUtil.cloneValue(cell)
        result += CellValue(string(CellUtil.cloneFamily(cell)), string(CellUtil.cloneQualifier(cell)),
          bValue, cell.getTimestamp)
    }
    result
  }

  override def getRow(rowKey: RowKey): Seq[CellValue] = {
    val get = new Get(bytes(rowKey.rowKey))
    val userTable = getTable(rowKey.tableName)
    val rs = userTable.get(get)
    val cells = rs.listCells()
    getCellValues(cells)
  }

  override def getRow(rowKey: RowByteKey): Seq[CellValue] = {
    val get = new Get(rowKey.rowKey)
    val userTable = getTable(rowKey.tableName)
    val rs = userTable.get(get)
    val cells = rs.listCells()
    getCellValues(cells)
  }

  override def scan(rowKeyRange: RowKeyRange): mutable.Map[String, Seq[CellValue]] = {
    val scan = new Scan
    // 默认设置为100，可以修改为通过配置修改
    //    scan.setBatch(100)
    scan.setStartRow(bytes(rowKeyRange.startRowKey))
    scan.setStopRow(bytes(rowKeyRange.stopRowKey))

    val userTable = getTable(rowKeyRange.tableName)
    val scanner = userTable.getScanner(scan)
    val result = new mutable.HashMap[String, Seq[CellValue]]
    for (row <- scanner.asScala) {
      result(string(row.getRow)) = getCellValues(row.listCells)
    }
    result
  }

  override def scanByPageFilter(tableName: String): mutable.Map[Array[Byte], Seq[CellValue]] = {
    val scan = new Scan
    scan.setFilter(new PageFilter(1))

    val userTable = getTable(tableName)
    val scanner = userTable.getScanner(scan)
    val result = new mutable.HashMap[Array[Byte], Seq[CellValue]]
    for (row <- scanner.asScala) {
      result(row.getRow) = getCellValues(row.listCells)
    }
    result
  }

  private def getPutByCellValues(key: String, values: Seq[CellValue]): Put = {

    val put = new Put(bytes(key))
    values.foreach {
      item =>
        put.addColumn(bytes(item.columnFamily), bytes(item.qualifier), item.value)
    }
    put
  }

  override def put(key: RowKey, values: Seq[CellValue]): Boolean = {

    val userTable = getTable(key.tableName)
    try {
      val put = getPutByCellValues(key.rowKey, values)
      userTable.put(put)
      return true
    } catch {
      case e: IOException => log.error(e.getMessage, e)
    }
    false
  }

  override def delete(key: RowKey): Unit = {
    val userTable = getTable(key.tableName)
    val delete = new Delete(bytes(key.rowKey))
    userTable.delete(delete)
  }

  override def batch(tableName: String, batchValues: Seq[BatchCellValue]): Seq[_] = {
    batch2(tableName, batchValues)
  }

  /**
    * 原始批处理操作
    *
    * @param tableName
    * @param batchValues
    * @return
    */
  def batch1(tableName: String, batchValues: Seq[BatchCellValue]): Seq[_] = {
    val userTable = getTable(tableName)
    val rows = new util.ArrayList[Row]
    for (value <- batchValues) {
      value.batchType match {
        case BatchType.ADD => rows.add(getPutByCellValues(value.rowKey, value.cellValues))
        case BatchType.DELETE => rows.add(new Delete(bytes(value.rowKey)))
      }
    }
    val result = new Array[Object](rows.size())
    userTable.batch(rows, result)
    result
  }

  private def getPutByCellValues2(key: String, values: Seq[CellValue]): Put = {

    val put = new Put(bytes(key))
    values.foreach {
      item =>
        put.addImmutable(bytes(item.columnFamily), bytes(item.qualifier), if (item.value != null) item.value
        else HConstants.EMPTY_BYTE_ARRAY)
    }
    put
  }

  /**
    * 按照Thrift接口的批处理操作
    *
    * @param tableName
    * @param batchValues
    * @return
    */
  def batch2(tableName: String, batchValues: Seq[BatchCellValue]): Seq[_] = {
    val userTable = getTable(tableName)
    val puts = new util.ArrayList[Put]
    val deletes = new util.ArrayList[Delete]
    for (value <- batchValues) {
      value.batchType match {
        case BatchType.ADD => puts.add(getPutByCellValues2(value.rowKey, value.cellValues))
        case BatchType.DELETE =>
      }
    }
    userTable.put(puts)
    null
  }

  override def close(): Unit = {
    conn.close()
  }

}
