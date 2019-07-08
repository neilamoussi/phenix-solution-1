package fr.carrefour.phenix.file

import scala.io.Source

object FileReader {
  def readFile(path: String): Stream[String] = {
    Source.fromFile(path).getLines().toStream
  }

}
