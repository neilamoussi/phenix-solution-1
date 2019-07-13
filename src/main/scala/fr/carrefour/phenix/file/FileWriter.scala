package fr.carrefour.phenix.file

import java.io.PrintWriter

import fr.carrefour.phenix.model.{DaySales, WeekTurnover}
import fr.carrefour.phenix.utils.Utils

object FileWriter {
  val LINE_SEPARATOR: String = "\r\n"
  val EXTENSION: String = ".data"
  val J7_SUFFIX: String = "-J7"
  val SALES: String = "ventes"
  val TOP_100: String = "top_100"
  val TURNOVER: String = "ca"
  val SEPARATOR: String = "|"

  // Return bool
  def writeDaySales(date: String, shopUuid: String, content: Stream[DaySales]): Unit = {
    val outputFilePath = generateDayTopSalesFileName(date, shopUuid)
    val writer = new PrintWriter(outputFilePath)
    content.foreach(content => writer.write(content.shopUuid + SEPARATOR +
      content.date + SEPARATOR +
      content.productId + SEPARATOR +
      content.quantity + LINE_SEPARATOR))
    writer.close()
  }

  def writeWeekTurnover(date: String, shopUuid: String, content: Stream[WeekTurnover]): Unit = {
    val outputFilePath = generateWeekTopTurnoverFileName(date, shopUuid)
    val writer = new PrintWriter(outputFilePath)
    content.foreach(content => writer.write(content.shopUuid + SEPARATOR +
      content.date + SEPARATOR +
      content.productId + SEPARATOR +
      content.turnover + LINE_SEPARATOR))
    writer.close()
  }

  def splitFile(input: Iterator[String], output_path: String, linesPerFile: Int): Unit = {
    if (input.hasNext) {
      val header = List(input.next())
      for ((i, lines) <- Iterator.from(1) zip input.grouped(linesPerFile)) {

        val out = new PrintWriter(s"${output_path}_part_${i}")
        (header.iterator ++ lines.iterator).foreach(out.println)
        out.close
      }
    }
  }

  def generateDayTopSalesFileName(date: String, shopUuid: String): String = {
    s"data/output/${TOP_100}_${SALES}_${shopUuid}_${Utils.dateToString(date)}$EXTENSION"
  }

  def generateWeekTopTurnoverFileName(date: String, shopUuid: String): String = {
    s"data/output/${TOP_100}_${TURNOVER}_${shopUuid}_${date}$J7_SUFFIX$EXTENSION"
  }

}