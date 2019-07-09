package fr.carrefour.phenix

import fr.carrefour.phenix.core.{ProductComputer, TransactionComputer}
import fr.carrefour.phenix.file.FileManager

object PhenixApp extends PhenixAppOptions {


  def process(inputFolder: String): Unit = {
    val fileManager = new FileManager()
    val transactionComputer = new TransactionComputer()

    val streamTransactions = fileManager.readTransactions(inputFolder)
    val streamProducts = fileManager.readProducts(inputFolder)

    val streamAsTransactions = transactionComputer.transformToTransactions(streamTransactions)
    val streamAsProducts = ProductComputer.transformToProducts(streamProducts)

    val daySales = transactionComputer.getDaysSales(streamAsTransactions)
    val topSales = transactionComputer.getDayTopSales(daySales)
    topSales.foreach(println)

    val dayShopTurnover = transactionComputer.getShopDaysTurnover(streamAsTransactions, streamAsProducts)
    val weekTopTurnover = transactionComputer.getWeekTopTurnover(dayShopTurnover)
    weekTopTurnover.foreach(println)
  }

    val options = parseCmdLineArgs(args)
    val inputFolder  =  options(inputFolderOption)
    process(inputFolder)


}
