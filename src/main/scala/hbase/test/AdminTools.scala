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

import hbase.test.utils.Util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.master.RegionState.State
import org.apache.hadoop.hbase.regionserver.BloomType

class AdminTools(zkQuorum: String) extends Util {

  def open(): Connection = {
    val cfg: Configuration = HBaseConfiguration.create()
    cfg.set("hbase.zookeeper.quorum", zkQuorum)
    ConnectionFactory.createConnection(cfg)
  }

  def createTable(tableName: String, columnFamily: String, splits: Array[Array[Byte]], isMOB: Boolean): Unit = {
    val admin = open().getAdmin
    if (admin.tableExists(TableName.valueOf(tableName))) {
      log.error(s"'$tableName'table Exists.")
      return
    }
    val descriptor = new HTableDescriptor(TableName.valueOf(tableName))

    val hcd = new HColumnDescriptor(columnFamily)
    // 设置MOB
    if (isMOB) {
      hcd.setMobEnabled(true)
      hcd.setMobThreshold(102400L)
      descriptor.setConfiguration("hfile.format.version", "3")
      descriptor.setConfiguration("hbase.hregion.max.filesize", (100L * 1024 * 1024 * 1024).toString)
    }
    hcd.setBloomFilterType(BloomType.NONE)
    descriptor.addFamily(hcd)


    if (splits == null) {
      admin.createTable(descriptor)
    } else {
      admin.createTable(descriptor, splits)
    }
    admin.close()
  }

  def alertTableRemoveAttr(tableName: String, columnFamily: String): Unit = {
    val admin = open().getAdmin
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      log.error(s"'$tableName'table is not Exists.")
      return
    }
    val tableNameObj = TableName.valueOf(tableName)
    val descriptor = admin.getTableDescriptor(tableNameObj)
    val families = descriptor.getColumnFamilies
    val family = families.find(p =>
      columnFamily.equals(string(p.getName))
    ).get
    //    family.remove(HColumnDescriptor.BLOCKSIZE.getBytes)
    //    family.remove(HColumnDescriptor.COMPRESSION_COMPACT.getBytes)
    family.remove(HColumnDescriptor.DATA_BLOCK_ENCODING.getBytes)
    admin.modifyTable(tableNameObj, descriptor)
    admin.close()
  }

  def getSplitingRegions(): Unit = {
    val admin = open().getAdmin
    import scala.collection.convert.decorateAsScala._
    val regions = admin.getClusterStatus.getRegionsInTransition
    for (elem <- regions.asScala) {
      elem._2.getState match {
        case State.SPLIT | State.SPLITTING | State.SPLITTING_NEW =>
          println(elem._2.getRegion.getTable)
      }
    }
    admin.close()
  }

  def getRegions(tableName: String): Unit = {
    val admin = open().getAdmin
    import scala.collection.convert.decorateAsScala._
    val tableNameObj = TableName.valueOf(tableName)
    val regions = admin.getTableRegions(tableNameObj).asScala
    regions.filter(r => r.getEncodedName.equals("")).map(println)
    admin.split(tableNameObj)
    admin.close()
  }

  def createTableWithSplit(tableName: String, columnFamily: String, regionsCount: Int): Unit = {
    val splits = new Array[Array[Byte]](regionsCount - 1)
    for (index <- 1 until regionsCount) {
      splits(index - 1) = "%03d".format(index).getBytes
    }
    createTable(tableName, columnFamily, splits, isMOB = true)
  }
}

object AdminTools {
  def main(args: Array[String]): Unit = {
    //    val adminTools = new AdminTools("gs-server-v-129,gs-server-v-128,gs-server-v-127")
    val adminTools = new AdminTools("gs-server-1028,gs-server-1032,gs-server-1031,gs-server-1027,gs-server-1033")
    //    adminTools.createTableWithSplit("wxx-test-100", "f", 30)

    //    adminTools.alertTableRemoveAttr("wxx-test-100", "f")

    //    adminTools.getSplitingRegions()

//    adminTools.getRegions("mediad:MediaD_WeixinRawData")


    val template = HBaseDaoTestTemplate(InterfaceType.JNI, "gs-server-1028,gs-server-1032,gs-server-1031,gs-server-1027,gs-server-1033")
    template.testGet(RowKey("wangxiaoxi", "rk0001"))
  }
}
