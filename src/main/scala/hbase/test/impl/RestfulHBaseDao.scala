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

import hbase.test._
import hbase.test.utils.Util
import org.apache.commons.lang.StringUtils
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPut, HttpRequestBase}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import sun.misc.{BASE64Decoder, BASE64Encoder}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

class RestfulHBaseDao(val host: String, val port: Int) extends HBaseDao {

  require(StringUtils.isNotBlank(host))
  require(port > 0)

  private var cm: PoolingHttpClientConnectionManager = _
  private val hostport = s"http://$host:$port/"

  override def open(): Unit = {
    if (cm == null) {
      cm = new PoolingHttpClientConnectionManager()
      cm.setMaxTotal(100) //整个连接池最大连接数
      cm.setDefaultMaxPerRoute(5) //每个路由最大连接数，默认值为2
    }
  }

  /**
    * 通过连接池获取HttpClient
    *
    * @return
    */
  private def getHttpClient: CloseableHttpClient = {
    open()
    HttpClients.custom().setConnectionManager(cm).build()
  }

  override def get(key: GetKey): Option[Array[Byte]] = {
    val url = s"$hostport/${key.tableName}/${key.rowKey}/${key.columnFamily}:${key.qualifier}"
    val request = new HttpGet(url)
    request.setHeader("Accept", "application/json")
    val result = RestfulHBaseDao.parseJson2CellValue(getResult(request))

    if (result.isEmpty) {
      None
    } else {
      Some(result.head.value)
    }
  }

  private def getRowByUrl(url: String): Seq[CellValue] = {
    val request = new HttpGet(url)
    // 通过json格式获取结果
    request.setHeader("Accept", "application/json")
    RestfulHBaseDao.parseJson2CellValue(getResult(request))
  }

  override def getRow(rowKey: RowKey): Seq[CellValue] = {
    //tableName/scanner/rowKey
    val url = s"$hostport/${rowKey.tableName}/${rowKey.rowKey}"
    getRowByUrl(url)
  }

  override def getRow(rowKey: RowByteKey): Seq[CellValue] = {
    //tableName/scanner/rowKey
    val url = s"$hostport/${rowKey.tableName}/${rowKey.rowKey}"
    getRowByUrl(url)
  }

  private def getScannerUrl(rowKeyRange: RowKeyRange): String = {
    val url = s"$hostport/${rowKeyRange.tableName}/scanner"
    val request = new HttpPut(url)
    request.setHeader("Content-Type", "text/xml")

    val startRowKey = RestfulHBaseDao.encode(rowKeyRange.startRowKey)
    val stopRowKey = RestfulHBaseDao.encode(rowKeyRange.stopRowKey)
    val scannerInfo = s"""<Scanner startRow="$startRowKey" endRow="$stopRowKey" />"""
    log.debug(scannerInfo)

    val entity = new StringEntity(scannerInfo, RestfulHBaseDao.CHARSET_ENCODING) //解决中文乱码问题
    entity.setContentEncoding(RestfulHBaseDao.CHARSET_ENCODING)
    request.setEntity(entity)

    val response = getHttpClient.execute(request)
    response.headerIterator().nextHeader().getValue
  }

  private def getRowsByUrl(url: String): mutable.Map[String, Seq[CellValue]] = {
    val request = new HttpGet(url)
    // 通过json格式获取结果
    request.setHeader("Accept", "application/json")
    RestfulHBaseDao.parseJson2Rows(getResult(request))
  }

  override def scan(rowKeyRange: RowKeyRange): mutable.Map[String, Seq[CellValue]] = {
    val scannerUrl = getScannerUrl(rowKeyRange)
    getRowsByUrl(scannerUrl)
  }

  override def put(rowKey: RowKey, values: Seq[CellValue]): Boolean = {
    val url = s"$hostport/${rowKey.tableName}/${rowKey.rowKey}"
    val request = new HttpPut(url)
    request.setHeader("Content-Type", "application/json")
    //获取json
    val json = RestfulHBaseDao.parseCellValues2Json(rowKey.rowKey, values)
    //    println(json)

    val entity = new StringEntity(json, RestfulHBaseDao.CHARSET_ENCODING) //解决中文乱码问题
    entity.setContentEncoding(RestfulHBaseDao.CHARSET_ENCODING)
    entity.setContentType("application/json")
    request.setEntity(entity)

    log.debug(s"RESULT:${getResult(request)}")

    true
  }

  override def delete(key: RowKey): Unit = {
    val url = s"$hostport/${key.tableName}/${key.rowKey}"
    val request = new HttpDelete(url)
    getResult(request)
  }

  override def batch(tableName: String, batchValues: Seq[BatchCellValue]): Seq[_] = {

    // HBase不支持批量操作
    /*val url = s"$hostport/$tableName"
    val request = new HttpPut(url)
    request.setHeader("Content-Type", "application/json")
    //获取json
    val json = RestfulHBaseDao.parseBatchRows2Json(batchValues)
    //    println(json)

    val entity = new StringEntity(json, RestfulHBaseDao.CHARSET_ENCODING) //解决中文乱码问题
    entity.setContentEncoding(RestfulHBaseDao.CHARSET_ENCODING)
    entity.setContentType("application/json")
    request.setEntity(entity)

    log.debug(s"RESULT:${getResult(request)}")*/

    //使用foreach put代替
    for (value <- batchValues) {
      value.batchType match {
        case BatchType.ADD =>
          put(RowKey(tableName, value.rowKey), value.cellValues)
        case BatchType.DELETE =>
          delete(RowKey(tableName, value.rowKey))
      }
    }
    null
  }

  /**
    * 处理Http请求.
    *
    * @param request 请求对象.
    * @return
    */
  private def getResult(request: HttpRequestBase): String = {
    val client = getHttpClient
    try {
      println(s"开始请求：request:${request.getURI}")
      val response = client.execute(request)
      val entity = response.getEntity

      if (entity != null) {
        // long len = entity.getContentLength()// -1 表示长度未知
        val result = EntityUtils.toString(entity)
        println(s"请求完成：result" + result)
        return result
      }
    } catch {
      case e: ClientProtocolException => e.printStackTrace()
      case ie: IOException => ie.printStackTrace()
    } finally {
      //      client.close()
    }

    RestfulHBaseDao.EMPTY_STR
  }

  override def close(): Unit = {
    cm.close()
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

object RestfulHBaseDao extends Util {

  val EMPTY_STR = ""
  val decoder = new BASE64Decoder
  val encoder = new BASE64Encoder
  val CHARSET_ENCODING = "UTF-8"

  def decodeValue2Str(str: String): String = {
    string(decoder.decodeBuffer(str))
  }

  def decodeValue2Bytes(str: String): Array[Byte] = {
    decoder.decodeBuffer(str)
  }

  def encode(str: String): String = {
    encoder.encode(bytebuffer(str))
  }

  /**
    * 通过JSON解析器，解析json数据
    *
    * @param json json数据.
    * @return 解析后的数据
    */
  private def parseJson2Map(json: String): Map[String, Any] = {
    val full = JSON.parseFull(json)
    full match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(map: Map[String, Any]) => map
      case None => log.warn("Parsing failed")
        throw new IllegalArgumentException("Parsing failed")
      case other => log.warn("Unknown data structure: " + other)
        throw new IllegalArgumentException("Unknown data structure: " + other)
    }
  }

  /**
    *
    * 按照数据格式获取结果，并封装到具体结果对象CellValue中
    * 以下为Json数据格式：
    *
    * {"Row":[
    * {"key":"cmswMDAy","Cell":[
    * {"column":"Y2YxOmFnZQ==",
    * "timestamp":1483686256323,
    * "$":"AAAAGA=="
    * },
    * {"column":"Y2YxOm5hbWU=",
    * "timestamp":1483686256323,
    * "$":"Y2hhbmdrdW4="
    * }
    * ]}
    * ]}
    *
    * @param json json数据
    * @return
    */
  private def parseJson2CellValue(json: String): Seq[CellValue] = {
    if (json.isEmpty)
      return null
    //解析json数据
    val map = parseJson2Map(json)
    val head = map("Row").asInstanceOf[Seq[_]].head.asInstanceOf[Map[String, _]]
    parseRowJson(head)
  }

  private def parseJson2Rows(json: String): mutable.Map[String, Seq[CellValue]] = {
    if (json.isEmpty)
      return null
    //解析json数据
    val map = parseJson2Map(json)
    val result = new mutable.HashMap[String, Seq[CellValue]]
    map("Row").asInstanceOf[Seq[_]].foreach { row =>
      val realRow = row.asInstanceOf[Map[String, _]]
      val keyStr = decodeValue2Str(realRow.get("key").asInstanceOf[Some[String]].get)
      result(keyStr) = parseRowJson(realRow)
    }
    result
  }

  private def parseRowJson(row: Map[String, _]) = {
    val cells = row.get("Cell").asInstanceOf[Some[_]].get.asInstanceOf[Seq[_]]
    val result = new ListBuffer[CellValue]
    cells.foreach {
      cell =>
        val cols = decodeValue2Str(cell.asInstanceOf[Map[String, String]]("column"))
        val col = cols.split(":")
        val timestamp = cell.asInstanceOf[Map[String, Double]]("timestamp")
        val value = decodeValue2Bytes(cell.asInstanceOf[Map[String, String]]("$"))
        result += CellValue(col(0), col(1), value, timestamp.toLong)
    }
    result
  }

  private def parseRow2Json(sb: StringBuilder, rowKey: String, list: Seq[CellValue]): Unit = {
    sb.append(s"""{"key":"${encoder.encode(bytes(rowKey))}","Cell":[""")
    list.foreach {
      cell =>
        val columnStr = s"${cell.columnFamily}:${cell.qualifier}"
        sb.append(s"""{"column":"${encoder.encode(bytes(columnStr))}","$$":"${encoder.encode(cell.value)}"},""")
    }
    sb.deleteCharAt(sb.length - 1).append("]},")
  }

  private def parseCellValues2Json(rowKey: String, list: Seq[CellValue]): String = {

    val sb = new StringBuilder("""{"Row":[""")
    parseRow2Json(sb, rowKey, list)
    sb.deleteCharAt(sb.length - 1).append("]}")
    sb.toString
  }

  private def parseBatchRows2Json(batch: Seq[BatchCellValue]): String = {

    val sb = new StringBuilder(s"""{\"Row\":[""")
    for (batchValue <- batch) {
      parseRow2Json(sb, batchValue.rowKey, batchValue.cellValues)
    }
    sb.append("]}")
    sb.toString
  }

  def apply(host: String, port: Int): RestfulHBaseDao = new RestfulHBaseDao(host, port)
}
