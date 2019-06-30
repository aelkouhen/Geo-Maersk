package com.maersk.geo

object Geolocalization extends InitSpark {

  def main(args: Array[String]) {
    import org.apache.spark.sql.functions._

    //Before Launching this program, you need to unzip the geo package in the project root.

    val codeList = reader.csv("geo_unlocode/code-list.csv")
    //Since text files have a header and delimited by ',' we can read them as csv files !
    val countries = reader.csv("geo_unlocode/country.txt")
    val functions = reader.csv("geo_unlocode/function_list.txt")


    //First left outer join between codeList and countries.
    val firstJoinedDF = codeList.join(countries,
      codeList.col("Country") === countries.col("CountryCode"),
      "left_outer")


    //second left outer join between the previous dataframe and functions file content.
    val secondJoinedDF = firstJoinedDF.join(functions,
      firstJoinedDF.col("Function").contains(functions.col("FunctionCode")),
      "left_outer")


    //Select relevant columns as required in the email: #Country_code | country_name | Location_code | Location_Name | Location_Type | Longitude | Latitude (Coordinates Splitted)
    val filteredResult = secondJoinedDF.withColumn("split", split(col("Coordinates"), " "))
      .select(col("Country").as("Country_code"),
        col("CountryName").as("Country_name"),
        col("Location").as("Location_code"),
        col("Name").as("Location_Name"),
        col("FunctionDescription").as("Location_Type"),
        col("split")(0).as("Longitude"),
        col("split")(1).as("Latitude"))

    //Perform conversion (from DMS to DD) on Latitudes and Longitudes.
    val calculatedResult = filteredResult
      .withColumn("Longitude", ConversionUtil.conversionUdf(col("Longitude")))
      .withColumn("Latitude", ConversionUtil.conversionUdf(col("Latitude")))
      .select(col("Country_code"),
        col("Country_name"),
        col("Location_code"),
        col("Location_Name"),
        col("Location_Type"),
        col("Longitude"),
        col("Latitude"))

    //An Overview of the result.
    calculatedResult.show(10)

    //The complete result will be generated as csv file inside the final_dataset folder
    calculatedResult.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save("final_dataset")

    close
  }
}
