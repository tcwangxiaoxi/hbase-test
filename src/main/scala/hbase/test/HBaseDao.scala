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

import hbase.test.BatchType.BatchType
import hbase.test.utils.Util

import scala.collection.mutable

/**
  * 用于实现与HBase进行交互的接口，包含了所有基础的操作方法
  */
trait HBaseDao extends Util {

  /**
    * 打开/建立连接
    */
  def open(): Unit

  /**
    * 初始化，如JNI方式的Table对象，可以在这里初始化，
    * 防止在操作调用时，算入操作的计算时间内
    *
    * @param params
    */
  def init(params: Map[String, String]): Unit

  /**
    * 获取指定Key的值
    *
    * @param key 包含具体表名、行标识、列族、列名信息的实体
    * @return 返回具体的值
    */
  def get(key: GetKey): Option[Array[Byte]]

  /**
    * 获取指定行的所有列族的列信息
    *
    * @param rowKey 包含表名、行标识的实体
    * @return 返回指定行的所有相关信息
    */
  def getRow(rowKey: RowKey): Seq[CellValue]

  /**
    * 获取指定行的所有列族的列信息
    *
    * @param rowKey 包含表名、行标识的实体
    * @return 返回指定行的所有相关信息
    */
  def getRow(rowKey: RowByteKey): Seq[CellValue]

  /**
    * 扫描指定数据范围内的数据信息
    *
    * @param rowKeyRange 包含表名，以及行标识的起始与结束范围
    * @return 返回指定范围内的所有相关信息
    *         Map(行标识, Seq(列值))
    */
  def scan(rowKeyRange: RowKeyRange): mutable.Map[String, Seq[CellValue]]

  /**
    * 插入指定行标识的相关数据
    *
    * @param key    行标识对象
    * @param values 列值对象
    * @return 插入结果
    */
  def put(key: RowKey, values: Seq[CellValue]): Boolean

  /**
    * 根据指定的行标识删除数据
    *
    * @param key 行标识
    */
  def delete(key: RowKey): Unit

  /**
    * 批量插入或删除一组HBase操作
    *
    * @param tableName   表名
    * @param batchValues 操作列表
    * @return 返回结果列表，根据操作列表的顺序，返回结果
    */
  def batch(tableName: String, batchValues: Seq[BatchCellValue]): Seq[_]

  /**
    * 关闭连接
    */
  def close(): Unit

  def scanByPageFilter(tableName: String): mutable.Map[Array[Byte], Seq[CellValue]]

}

/**
  * 用于表示特定单元的实体对象
  *
  * @param tableName    表名
  * @param rowKey       行标识
  * @param columnFamily 列族
  * @param qualifier    列名
  */
case class GetKey(tableName: String, rowKey: String, columnFamily: String, qualifier: String) {
  val fullColumnName: String = columnFamily + ":" + qualifier
}

/**
  * 用于表示行标识的实体对象
  *
  * @param tableName 表名
  * @param rowKey    行标识
  */
case class RowKey(tableName: String, rowKey: String)

/**
  * 用于表示行标识的实体对象
  *
  * @param tableName 表名
  * @param rowKey    行标识
  */
case class RowByteKey(tableName: String, rowKey: Array[Byte])

/**
  * 用于表示指定单元的值
  *
  * @param columnFamily 列族
  * @param qualifier    列名
  * @param value        值，以字节数组的形式表示
  * @param timestamp    时间戳，当插入数据操作的时候默认为系统是时间，获取数据操作的时候，默认为最新的一个值
  */
case class CellValue(columnFamily: String, qualifier: String, value: Array[Byte], timestamp: Long)

/**
  * 用于表示一个查询范围
  *
  * @param tableName   表名
  * @param startRowKey 开始的行标识
  * @param stopRowKey  结束的行标识
  */
case class RowKeyRange(tableName: String, startRowKey: String, stopRowKey: String)

/**
  * 用于表示批处理操作中的一行数据
  *
  * @param rowKey     行标识
  * @param batchType  操作类型，分为插入和删除
  * @param cellValues 列值列表
  */
case class BatchCellValue(rowKey: String, batchType: BatchType, cellValues: Seq[CellValue])