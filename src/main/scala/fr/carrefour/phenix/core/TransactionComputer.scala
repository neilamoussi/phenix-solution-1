package fr.carrefour.phenix.core

import java.time.LocalDate

import fr.carrefour.phenix.file.FileWriter
import fr.carrefour.phenix.model._
import fr.carrefour.phenix.utils.Utils

class TransactionComputer {

  def transformToTransaction(line: String): Transaction = {
    line.split(Utils.SEPARATOR) match {
      case Array(transactionId, date, shopUuid, productId, quantity) =>
        Transaction(transactionId.toInt, LocalDate.parse(date.take(8), Utils.YYYYMMDD_FORMAT),
          shopUuid, productId.toInt, quantity.toInt)
    }
  }

  def transformToTransactions(transactions: Stream[String]): Stream[Transaction] = {
    transactions.map(line => transformToTransaction(line))
  }


  def getDaysSales(transactions: Stream[Transaction]): Stream[DaySales] = {
    transactions
      .groupBy(transaction => (transaction.shopUuid, transaction.datetime, transaction.productId))
      .map(sales => DaySales(sales._1._1, sales._1._2, sales._1._3, sales._2.map(_.quantity).sum))
      .toStream
  }

  // Return stream or bool for success state, i need to make a choice
  def getShopDayTopSales(date: LocalDate, shopUuid: String, daySales: Stream[DaySales]): Stream[DaySales] = {

    val dayTopSales = daySales.filter(sales => sales.date.compareTo(date) == 0 && sales.shopUuid == shopUuid)
      .sortBy(_.quantity)(Ordering[Int].reverse)
      .take(100)

   FileWriter.writeDaySales(date.toString, shopUuid, dayTopSales)
    dayTopSales
  }

  def getDayTopSales(daySales: Stream[DaySales]): Stream[DaySales] = {
    daySales.map(sales => (sales.shopUuid, sales.date))
      .distinct
      .flatMap(sales => getShopDayTopSales(sales._2, sales._1, daySales))

  }

  // input le bon stream de produit
  def getShopDaysCa(transactions: Stream[Transaction], products: Map[String, Stream[Product]]): Stream[DayCA] = {
    getDaysSales(transactions)
      .map(sales => computeDayCA(sales, products))
  }

  def computeDayCA(daySales: DaySales, products: Map[String, Stream[Product]]): DayCA = {
    DayCA(daySales.shopUuid,
      daySales.date,
      daySales.productId,
      daySales.quantity,
      ProductComputer.getPrice(daySales, products)
    )
  }

  def computeWeekCA(dayCA: DayCA): WeekCA = {
    WeekCA(dayCA.shopUuid, dayCA.date, dayCA.productId, dayCA.ca)
  }

  def getShopWeekTopCA(shopUuid: String, weekCA: Stream[WeekCA]): Stream[WeekCA] = {
    val weekTopCA = weekCA.filter(sales => sales.shopUuid == shopUuid)
      .sortBy(_.ca)(Ordering[Double].reverse)
      .take(100)
    FileWriter.writeCA(Utils.now.toString, shopUuid, weekTopCA)
    weekTopCA
  }

  def getWeekTopCA(dayCA: Stream[DayCA]): Stream[WeekCA] = {
    val weekCA = dayCA.filter(sales => Utils.compareTo7(sales.date))
      .groupBy(sales => (sales.shopUuid, sales.productId))
      .mapValues(sales => sales.map(_.ca).sum)
      .map(sales => WeekCA(sales._1._1, Utils.now, sales._1._2, sales._2 ))
      .toStream

    weekCA.map(sales => sales.shopUuid)
      .distinct
      .flatMap(shopUuid => getShopWeekTopCA(shopUuid, weekCA))

  }


}
