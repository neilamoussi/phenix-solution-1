package fr.carrefour.phenix.model

import java.time.LocalDate

case class Transactions(transactions: Stream[Transaction])
case class DaySales(shopUuid: String, date: LocalDate, productId: Int, quantity: Int)
case class DayTurnover(shopUuid: String, date: LocalDate, productId: Int, quantity: Int, turnover: Double)
case class WeekTurnover(shopUuid: String, date: LocalDate, productId: Int, turnover: Double)
