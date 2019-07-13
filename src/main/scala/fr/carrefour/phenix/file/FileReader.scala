package fr.carrefour.phenix.file

import java.io.File

import scala.io.Source

object FileReader {
  val MAX_LINES = 50000

  def readFile(path: String): Stream[String] = {

   val lines =  Source.fromFile(path).getLines()
    if (lines.size > MAX_LINES) {
      FileWriter.splitFile(Source.fromFile(path).getLines(), path, MAX_LINES - 1)
      println(s"You need to split ${path} file")
      new File(path).delete()
      Stream.empty[String]
    } else {
      Source.fromFile(path).getLines().toStream
    }
  }

}
