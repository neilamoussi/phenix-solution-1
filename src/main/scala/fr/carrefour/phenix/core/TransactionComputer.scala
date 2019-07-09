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

  def getShopDaysTurnover(transactions: Stream[Transaction], products: Map[String, Stream[Product]]): Stream[DayTurnover] = {
    getDaysSales(transactions)
      .map(sales => computeDayTurnover(sales, products))
  }

  def computeDayTurnover(daySales: DaySales, products: Map[String, Stream[Product]]): DayTurnover = {
    DayTurnover(daySales.shopUuid,
      daySales.date,
      daySales.productId,
      daySales.quantity,
      ProductComputer.getPrice(daySales, products)
    )
  }

  def computeWeekTurnover(dayTurnover: DayTurnover): WeekTurnover = {
    WeekTurnover(dayTurnover.shopUuid, dayTurnover.date, dayTurnover.productId, dayTurnover.turnover)
  }

  def getShopWeekTopTurnover(shopUuid: String, weekTurnover: Stream[WeekTurnover]): Stream[WeekTurnover] = {
    val weekTopTurnover = weekTurnover.filter(sales => sales.shopUuid == shopUuid)
      .sortBy(_.turnover)(Ordering[Double].reverse)
      .take(100)
    FileWriter.writeWeekTurnover(Utils.now.toString, shopUuid, weekTopTurnover)
    weekTopTurnover
  }

  def getWeekTopTurnover(dayTurnover: Stream[DayTurnover]): Stream[WeekTurnover] = {
    val weekTurnover = dayTurnover.filter(sales => Utils.compareTo7(sales.date))
      .groupBy(sales => (sales.shopUuid, sales.productId))
      .mapValues(sales => sales.map(_.turnover).sum)
      .map(sales => WeekTurnover(sales._1._1, Utils.now, sales._1._2, sales._2 ))
      .toStream

    weekTurnover.map(sales => sales.shopUuid)
      .distinct
      .flatMap(shopUuid => getShopWeekTopTurnover(shopUuid, weekTurnover))
  }

}
