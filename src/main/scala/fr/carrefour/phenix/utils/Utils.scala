package fr.carrefour.phenix.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.IsoFields

object Utils {
  val SEPARATOR: String = """\|"""
  val YYYYMMDD_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd")

  // TODO IN the real case study, you should use the current date
  val now = LocalDate.parse("20170614", YYYYMMDD_FORMAT)

  def computeWeek(date: LocalDate): Int = {
    date.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)
  }

  def dateToString(date: LocalDate): String = {
    dateToString(date.toString)
  }

  def dateToString(date: String): String = {
    date.replace("-", "")
  }

  def compareTo7(input_date: LocalDate): Boolean = {
    val beginDate = LocalDate.parse(dateToString(now), YYYYMMDD_FORMAT)
      .minusWeeks(1L)

    input_date.isBefore(now) || input_date.isEqual(now) && input_date.isAfter(beginDate)


  }

}
