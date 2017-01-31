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

package hbase.test.utils

import java.io._
import java.nio.ByteBuffer
import java.util.Date
import javax.swing.JPopupMenu.Separator

import com.sun.org.apache.commons.logging.{Log, LogFactory}
import hbase.test.{CellValue, GetKey, RowKey, RowKeyRange}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer
import scala.io.{Codec, Source}
import scala.util.Random

trait Util {

  val log: Log = LogFactory.getLog(this.getClass)

  def bytes(str: String): Array[Byte] = {
    Bytes.toBytes(str)
  }

  def string(bytes: Array[Byte]): String = {
    Bytes.toString(bytes)
  }

  def string(buffer: ByteBuffer): String = {
    string(buffer.array())
  }

  def bytebuffer(string: String): ByteBuffer = {
    ByteBuffer.wrap(bytes(string))
  }

  def bytebuffer(bytes: Array[Byte]): ByteBuffer = {
    ByteBuffer.wrap(bytes)
  }

  def getTableRow(key: RowKey): (ByteBuffer, ByteBuffer) = {
    (bytebuffer(key.tableName), bytebuffer(key.rowKey))
  }

  def getTableRow(key: GetKey): (ByteBuffer, ByteBuffer) = {
    (bytebuffer(key.tableName), bytebuffer(key.rowKey))
  }

  def getTableRow(key: RowKeyRange): (ByteBuffer, ByteBuffer, ByteBuffer) = {
    (bytebuffer(key.tableName), bytebuffer(key.startRowKey), bytebuffer(key.stopRowKey))
  }

  def saveAsFile(dir: String, key: Array[Byte], datas: Seq[CellValue]): Unit = {
    log.info("保存数据到：" + dir)

    val os = new FileOutputStream(new File(dir + "/key_" + key.length))
    os.write(key)
    os.close()

    datas.foreach { item =>
      val writer = new PrintWriter(new File(dir + "/" + item.columnFamily + "_" + item.qualifier))
      writer.write(string(item.value))
      writer.close()
    }
    log.info("保存完毕")
  }

  def readFromFile(dir: String): (Array[Byte], Seq[CellValue]) = {
    log.info(s"从${dir}中读取数据")

    val result = new ArrayBuffer[CellValue]
    var rowKey: Array[Byte] = null
    new File(dir).listFiles().foreach {
      file =>
        val pair = file.getName.split("_")
        val key = pair(0)
        val info = pair(1)
        log.info(s"读取数据：key:$key, info:$info")
        // 读取保存rowkey的文件
        if ("key".equals(key)) {
          val is = new FileInputStream(new File(file.getAbsolutePath))
          rowKey = new Array[Byte](info.toInt)
          is.read(rowKey)
          is.close()
        } else {
          // 读取列信息文件
          val source = Source.fromFile(file.getAbsolutePath)(Codec.UTF8)
          val sb = new StringBuilder()
          source.getLines().foreach {
            line =>
              sb.append(line)
          }
          result += CellValue(pair(0), pair(1), bytes(sb.toString()), 0)
          source.close()
        }
    }
    log.info("读取完毕")
    (rowKey, result)
  }

  def randomKey(index: Int): String = {
    Random.nextInt(32)
    val typeStr = "%02d".format(Random.nextInt(32))
    val num = "%010d".format(index)
    return Random.nextInt(32) + "bkt" + num
  }

  def readFromFile2Compact(src: String, dest: String, separator: String, size: Int): Unit = {

    val result = readFromFile(src)
    var curBatchIndex = 0
    val pw = new PrintWriter(new File(dest))
    log.info(s"从${src}中读取数据")
    try {
      // 循环写入文件，生成打的tsv文件，用于HBase的 Bulk Load导入测试
      for (i <- 0 until size) {
        val cells = result._2
        // 遍历每个字段，写入一行数据
        // 写入Key值
        pw.print(randomKey(i))
        pw.print(separator)
        // 写入列数据
        for (index <- cells.indices) {
          val cell = cells(index)
          val formatValue = string(cell.value).replaceAll("\t", " ")
          pw.print(formatValue)
          if (index != cells.size - 1) {
            // 如果不是最后一个字段，则写入分隔符
            pw.print(separator)
          } else {
            // 否则换行
            pw.println()
          }
        }
      }
    } catch {
      case e: Exception =>
        log.error(e)
        throw e
    } finally {
      pw.close()
    }
    log.info("循环写入数据完成")
  }
}

object App extends Util {
  def main(args: Array[String]): Unit = {
    //    testReadFromFile2Compact()
    println(new Date(1465543870237L).toLocaleString)
  }


  def testReadFromFile2Compact(): Unit = {

    /**
      * cf1:BinaryDocument,cf1:CrawledDepth,cf1:CrawlTime,cf1:Encoding,cf1:Entrance,
      * cf1:IsMultiplePage,cf1:RawPageList,cf1:RefUrl,cf1:Tags,cf1:TaskId,cf1:Url,cf1:WebSiteID
      */
    readFromFile2Compact("D:\\project\\hbase-test\\tempDatas", "D:\\project\\hbase-test\\bulkload.tsv", "\t", 10000)

  }
}

