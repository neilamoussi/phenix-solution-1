package fr.carrefour.phenix.model

import java.time.LocalDate

case class Transaction(transactionId: Int, datetime: LocalDate, shopUuid: String, productId: Int, quantity: Int)
case class Product(productId: Int, price: Double)




