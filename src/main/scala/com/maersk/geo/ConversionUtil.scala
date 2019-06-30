package com.maersk.geo

object ConversionUtil extends Serializable {

  import org.apache.spark.sql.functions._

  def conversionUdf = udf[String, String](convertDMSToDD)

  def convertDMSToDD(s: String): String = s match {
    case null => "NA"
    case _ => {
      val direction = s.substring(s.length - 1, s.length)
      var deg = 0.0
      var min = 0.0
      var sec = 0.0

      if (s.length > 5) {
        deg = s.substring(0, 3).toDouble
        min = s.substring(4, s.length - 1).toDouble
      }
      else {
        deg = s.substring(0, 2).toDouble
        min = s.substring(2, s.length - 1).toDouble
      }

      DMSToDecimal(direction, deg, min).toString
    }
  }

  /*
  * Conversion DMS to decimal
  *
  * Input: latitude or longitude in the DMS format ( example: W 79° 58' 55.903")
  * Return: latitude or longitude in decimal format
  * hemisphereOUmeridien => {W,E,S,N}
  *
  */
  def DMSToDecimal(direction: String, degrees: Double, minutes: Double): Double = {
    var LatOrLon = 0.0
    var polarity = 1.0
    if ((direction == "W") || (direction == "S")) polarity = -1.0
    LatOrLon = polarity * (degrees + ((minutes * 60) / 3600.0))

    LatOrLon
  }
}
