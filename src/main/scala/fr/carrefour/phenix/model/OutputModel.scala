package fr.carrefour.phenix.model

import java.time.LocalDate

case class Transactions(transactions: Stream[Transaction])
case class DaySales(shopUuid: String, date: LocalDate, productId: Int, quantity: Int)
case class DayCA(shopUuid: String, date: LocalDate,  productId: Int, quantity: Int, ca: Double)
case class WeekCA(shopUuid: String, date: LocalDate, productId: Int, ca: Double)
