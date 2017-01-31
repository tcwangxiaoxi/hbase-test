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

/**
  * 用于计算代码块的运行时间的工具类
  */
trait TimedUtil {
  @volatile
  var dummy: Any = _

  /**
    * 计算运行时间
    *
    * @param body 计算的代码块
    * @tparam T 计算代码的返回值
    * @return
    */
  def timed[T](body: => T): Double = {
    val start = System.nanoTime

    /**
      * JVM 中的某些运行时优化技术（如死代码消除），可能会去除调用body代码块的语句，
      * 使我们得到错误的运行时间。为了避免出现这种情况，可以将代码块body的返回值赋予
      * 名为 dummy 的 Volatile 字段。
      */
    dummy = body
    val end = System.nanoTime
    ((end - start) / 1000) / 1000.0
  }

  /**
    * 在程序代码块达到稳定状态前，可以通过制定次数的预计算，
    * 在总结程序运行时间的测量前，先确保JVM到达了稳定状态。
    * （因为JVM开始会通过解释模式执行代码，
    * 只有当JVM确定某些Java字节码被执行的次数足够多时，
    * 才会通过JIT实时编译（将这些Java字节码编译为机器码），在CPU中直接执行。）
    *
    * @param n    预处理次数
    * @param body 代码块
    * @tparam T 返回值类型
    * @return
    */
  def warmedTimed[T](n: Int = 200)(body: => T): Double = {
    for (_ <- 0 until n) body
    timed(body)
  }
}
