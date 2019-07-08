package fr.carrefour.phenix.model

import java.time.{LocalDate, LocalDateTime}


case class Transactions(transactions: Stream[Transaction])
case class DaySales(shopUuid: String, date: LocalDate, productId: Int, quantity: Int)
case class DayCA(shopUuid: String, date: LocalDate,  productId: Int, quantity: Int, ca: Double)
case class WeekCA(shopUuid: String, date: LocalDate, productId: Int, ca: Double)

/*
case class Transactions(transactions_20170614.data: Stream[Transaction], metaData: TransactionFileMetaData) extends Ordered[Transactions] {
  override def compare(ts2: Transactions): Int = this.metaData.date compareTo ts2.metaData.date
} */


