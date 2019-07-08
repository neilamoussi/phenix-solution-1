package fr.carrefour.phenix.file

import java.io.PrintWriter
import fr.carrefour.phenix.model.{DaySales, WeekCA}
import fr.carrefour.phenix.utils.Utils

object FileWriter {
  val LINE_SEPARATOR: String = "\r\n"
  val CA: String = "ca"
  val EXTENSION: String = ".data"
  val J7_SUFFIX: String = "-J7"
  val SALES: String = "ventes"
  val TOP_100: String = "top_100"
  val SEPARATOR: String = "|"

  def write(outputFilePath: String, content: Stream[String]): Unit = {

    val writer = new PrintWriter(outputFilePath)
    content.foreach(line => writer.write(line + LINE_SEPARATOR))
    writer.close()

  }

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

  def writeCA(date: String, shopUuid: String, content: Stream[WeekCA]): Unit = {
    val outputFilePath = generateWeekTopCaFileName(date, shopUuid)
    val writer = new PrintWriter(outputFilePath)
    content.foreach(content => writer.write(content.shopUuid + SEPARATOR +
      content.date + SEPARATOR +
      content.productId + SEPARATOR +
      content.ca + LINE_SEPARATOR))
    writer.close()
  }

  def generateDayTopSalesFileName(date: String, shopId: String): String = {
    s"data/output/${TOP_100}_${SALES}_${shopId}_${Utils.dateToString(date)}$EXTENSION"
  }

  def generateWeekTopCaFileName(date: String, shopId: String): String = {
    s"data/output/${TOP_100}_${CA}_${shopId}_${date}$J7_SUFFIX$EXTENSION"
  }

}