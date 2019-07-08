import java.time.LocalDate

import fr.carrefour.phenix.core.TransactionComputer
import fr.carrefour.phenix.model.{DaySales, Transaction, Product}
import fr.carrefour.phenix.utils.Utils
import org.scalatest.FunSuite

class TransactionComputerTest extends FunSuite{
  val transactionComputer = new TransactionComputer()

  test("Transform to transaction") {
    val line = "224|20170714T181127+0100|29366c83-eae9-42d3-a8af-f15339830dc5|74|200"
    val expected = Transaction(224,
      LocalDate.parse("20170714", Utils.YYYYMMDD_FORMAT),
      "29366c83-eae9-42d3-a8af-f15339830dc5", 74, 200)
    assert(transactionComputer.transformToTransaction(line) === expected)
  }

  test("Test top_100_sales_<SHOP_UUID>_day") {

    val transaction_1 : Transaction = Transaction(1, LocalDate.now(), "shop_1", 1, 1)
    val transaction_2 : Transaction = Transaction(2, LocalDate.now(), "shop_1", 2, 3)
    val transaction_3 : Transaction = Transaction(3, LocalDate.now(), "shop_1", 2, 10)
    val transaction_4 : Transaction = Transaction(1, LocalDate.now(), "shop_2", 4, 1)

    val streamTransaction: Stream[Transaction] = Seq(transaction_1, transaction_2, transaction_3, transaction_4)
      .toStream
    val daySales: Stream[DaySales] = transactionComputer.getDaysSales(streamTransaction)
    val topSales = transactionComputer.getDayTopSales(daySales)

    val nb_product = topSales.size
    val nb_product_shop_1 = topSales.filter(x => x.shopUuid =="shop_1").size
    val nb_max_sales_shop_1 = topSales.filter(x => x.shopUuid =="shop_1").map(x => x.quantity).max

    assert(nb_product === 3)
    assert(nb_product_shop_1 === 2)
    assert(nb_max_sales_shop_1 === 13)
  }

  test("Test  top_100_ca_<SHOP_UUID>_week") {

    val day_1 : LocalDate = LocalDate.parse("20170614", Utils.YYYYMMDD_FORMAT)
    val day_3 : LocalDate = day_1.minusDays(2L)


    val transaction_1 : Transaction = Transaction(1, day_1, "shop_1", 1, 1)
    val transaction_2 : Transaction = Transaction(2, day_1, "shop_1", 2, 2)
    val transaction_3 : Transaction = Transaction(3, day_1, "shop_1", 2, 8)
    val transaction_4 : Transaction = Transaction(1, day_1, "shop_1", 3, 1)

    val transaction_5 : Transaction = Transaction(1, day_1, "shop_2", 1, 1)
    val transaction_6 : Transaction = Transaction(1, day_1, "shop_2", 4, 1)
    val transaction_7 : Transaction = Transaction(2, day_3, "shop_2", 1, 1)

    val shop_1_product_1: Product = Product(1, 10.0)
    val shop_1_product_2: Product = Product(2, 5.0)
    val shop_1_product_3: Product = Product(3, 20.0)

    val shop_2_product_1: Product = Product(1, 100.0)
    val shop_2_product_1_bis: Product = Product(1, 10.0)
    val shop_2_product_4: Product = Product(4, 12.0)

    val streamTransaction: Stream[Transaction] = Seq(transaction_1, transaction_2, transaction_3,
      transaction_4, transaction_5, transaction_6, transaction_7).toStream

    val streamS1Products: Stream[Product] = Seq(shop_1_product_1, shop_1_product_2, shop_1_product_3).toStream
    val streamS2Products: Stream[Product] = Seq(shop_2_product_1, shop_2_product_4).toStream
    val streamS2Procuts_2D_BEFORE: Stream[Product] = Seq(shop_2_product_1_bis).toStream

    val mapProducts = Map("shop_1_" + Utils.dateToString(day_1)-> streamS1Products,
      "shop_2_" + Utils.dateToString(day_1) -> streamS2Products,
      "shop_2_" + Utils.dateToString(day_3) -> streamS2Procuts_2D_BEFORE)


    val dayShopCa = transactionComputer.getShopDaysCa(streamTransaction, mapProducts)
    val weekTopCA = transactionComputer.getWeekTopCA(dayShopCa)

    assert(weekTopCA.filter(x => x.shopUuid == "shop_1").size == 3)
    assert(weekTopCA.filter(x => x.shopUuid == "shop_1").map(x => x.ca).max == 10 * 5.0)
    assert(weekTopCA.filter(x => x.shopUuid == "shop_2").map(x => x.ca).max == 1 * 100.0 + 1 * 10.0)

  }
  // TODO writer & reader Test

}


