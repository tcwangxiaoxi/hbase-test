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

import java.nio.ByteBuffer
import java.util

import hbase.test._
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.thrift.generated.{BatchMutation, Hbase, Mutation, TRowResult}
import org.apache.hadoop.hbase.thrift2.generated.TPut
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class ThriftHBaseDao(host: String, port: Int) extends HBaseDao {

  require(StringUtils.isNotBlank(host))
  require(port > 0)

  private var client: Hbase.Client = _

  private var transport: TSocket = _

  override def open(): Unit = {
    val timeout = 10000
    println("host = " + host + ", port = " + port)
    val start = System.currentTimeMillis()
    var end = 0L
    transport = new TSocket(host, port, timeout)
    val protocol = new TBinaryProtocol(transport)

    client = new Hbase.Client(protocol)
    // open the transport
    transport.open()
    end = System.currentTimeMillis()
    System.out.println("Transport建立花费时间: " + (end - start) + " ms")
  }

  override def get(key: GetKey): Option[Array[Byte]] = {
    val (table, row) = getTableRow(key)

    val columns = new util.ArrayList[ByteBuffer]
    columns.add(bytebuffer(key.fullColumnName))
    val result = client.getRowWithColumns(table, row, columns, null)

    for (row <- result.asScala;
         column <- row.columns.asScala) {
      return Some(column._2.value.array())
    }
    None
  }

  private def getCells(rowResult: util.List[TRowResult]): Seq[CellValue] = {
    val cellValues = new ListBuffer[CellValue]
    for (row <- rowResult.asScala;
         column <- row.columns.asScala) {
      val columnVals = string(column._1.array()).split(":")
      val value = column._2.value.array()
      cellValues += CellValue(columnVals(0), columnVals(1), value, column._2.timestamp)
    }
    cellValues
  }

  override def getRow(rowKey: RowKey): Seq[CellValue] = {
    val (table, row) = getTableRow(rowKey)
    val rowResult = client.getRow(table, row, null)
    getCells(rowResult)
  }

  override def getRow(rowKey: RowByteKey): Seq[CellValue] = {
    val rowResult = client.getRow(bytebuffer(rowKey.tableName), bytebuffer(rowKey.rowKey), null)
    getCells(rowResult)
  }

  override def scan(rowKeyRange: RowKeyRange): mutable.Map[String, Seq[CellValue]] = {
    val (table, startRow, stopRow) = getTableRow(rowKeyRange)
    val id = client.scannerOpenWithStop(table, startRow, stopRow, null, null)
    val result = new mutable.HashMap[String, Seq[CellValue]]()
    while (true) {
      val rowResult = client.scannerGet(id)
      if (rowResult.isEmpty) {
        log.debug("Scanner finished")
        return result
      }
      val cellValues = getCells(rowResult)
      result(string(rowResult.get(0).row)) = cellValues
    }
    null
  }

  private def getMutationsByCellValues(values: Seq[CellValue], isDelete: Boolean): util.ArrayList[Mutation] = {
    val columnValues = new util.ArrayList[Mutation]
    values.foreach {
      cell =>
        val columnStr = s"${cell.columnFamily}:${cell.qualifier}"
        val columnValue = new Mutation(isDelete, bytebuffer(columnStr), bytebuffer(cell.value), true)
        columnValues.add(columnValue)
    }
    columnValues
  }

  override def put(key: RowKey, values: Seq[CellValue]): Boolean = {
    val (table, row) = getTableRow(key)
    val columnValues = getMutationsByCellValues(values, isDelete = false)
    client.mutateRow(table, row, columnValues, null)
    true
  }

  override def delete(key: RowKey): Unit = {
    val (table, row) = getTableRow(key)
    client.deleteAllRow(table, row, null)
  }

  override def batch(tableName: String, batchValues: Seq[BatchCellValue]): Seq[_] = {
    val batchMutation = new util.ArrayList[BatchMutation]
    batchValues.foreach {
      batchValue =>
        val isDelete = batchValue.batchType match {
          case BatchType.ADD => false
          case BatchType.DELETE => true
        }
        val bm = new BatchMutation(bytebuffer(batchValue.rowKey), getMutationsByCellValues(batchValue.cellValues, isDelete))
        batchMutation.add(bm)
    }
    client.mutateRows(bytebuffer(tableName), batchMutation, null)
    null
  }

  override def close(): Unit = {
    transport.close()
  }

  override def scanByPageFilter(tableName: String): mutable.Map[Array[Byte], Seq[CellValue]] = {
    null
  }

  /**
    * 初始化，如JNI方式的Table对象，可以在这里初始化，
    * 防止在操作调用时，算入操作的计算时间内
    *
    * @param params
    */
  override def init(params: Map[String, String]): Unit = {}
}
