package fr.carrefour.phenix.core

import fr.carrefour.phenix.model.{DaySales, Product}
import fr.carrefour.phenix.utils.Utils

object ProductComputer {

  def transformToProduct(line: String): Product = {
    line.split(Utils.SEPARATOR) match {
      case Array(productId, price) =>
        Product(productId.toInt, price.toDouble)
    }
  }

  def transformToProducts(products: Map[String, Stream[String]]): Map[String, Stream[Product]] = {
    products.map( products => (products._1, products._2.map(line => transformToProduct(line))))
  }


  def getPrice(sale: DaySales, products: Map[String, Stream[Product]]): Double = {
    val key = sale.shopUuid + "_" + Utils.dateToString(sale.date)
    val shopProducts = products.get(key).getOrElse(Stream.Empty)
    val price: Double = shopProducts.find(product => product.productId == sale.productId)
      .map(product => product.price)
      .getOrElse(0)
    price * sale.quantity

  }
}
