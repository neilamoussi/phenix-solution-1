package fr.carrefour.phenix.file

import java.io.File
import java.nio.file.Paths

import scala.util.matching.Regex

class FileManager {

  val PRODUCT_PATTERN: Regex = """^reference\_prod\-(.*)\_[0-9]{8}\.data$""".r
  val SHOPUUID_DATE_PATTERN: Regex = """-(.*)\.""".r
  val TRANSACTION_PATTERN: Regex = """^transactions_[0-9]{8}\.data$""".r


  def checkFolder(path: String): Boolean = {
    Paths.get(path).toAbsolutePath
      .toFile
      .isDirectory
  }

  def checkProducts(fileName: String): Boolean = {
    PRODUCT_PATTERN.findFirstIn(fileName).isDefined
  }

  def  checkTransactions(fileName: String): Boolean = {
    TRANSACTION_PATTERN.findFirstIn(fileName).isDefined
  }


  def setKeyProductsStream(fileName: String): String = {
    SHOPUUID_DATE_PATTERN.findAllIn(fileName).next().substring(1).init
  }

  def readTransactions(path: String): Stream[String] = {
    if (checkFolder(path)) {
      val file = new File(path)
      val filePaths = file.listFiles.filter(_.isFile)
        .filter(file => checkTransactions(file.getName))
        .map(_.getPath)
      filePaths.flatMap(filePath =>  FileReader.readFile(filePath)).toStream
    } else {
      Stream.Empty
    }
  }

  def readProducts(path: String): Map[String, Stream[String]] = {
    if (checkFolder(path)) {
      val file = new File(path)
      val filePaths = file.listFiles.filter(_.isFile)
        .filter(file => checkProducts(file.getName))
        .map(_.getPath)
      filePaths.map(filePath => (setKeyProductsStream(filePath), FileReader.readFile(filePath))).toMap
    } else {
      Map.empty[String, Stream[String]]
    }
  }





}
