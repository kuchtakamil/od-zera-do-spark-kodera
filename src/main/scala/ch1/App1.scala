package ch1

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object App1 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("load-movie-file")
      .master("local")
      .getOrCreate()

    val movieDf: DataFrame = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("src/main/resources/netflix.csv")

    movieDf.show()
    movieDf.printSchema()

    val movieSchema = StructType(Array(
      StructField("index", IntegerType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("title", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("release_year", IntegerType, nullable = true),
      StructField("age_certification", StringType, nullable = true),
      StructField("runtime", IntegerType, nullable = true),
      StructField("imdb_id", StringType, nullable = true),
      StructField("imdb_score", DoubleType, nullable = true),
      StructField("imdb_votes", DoubleType, nullable = true)
    ))

    // DataFrame, bez inferSchema, z przekazaną ręcznie stworzoną schemą movieSchema
    val movieDf2: DataFrame = spark.read
      .option("header", value = "true")
      .schema(movieSchema)
      .csv("src/main/resources/netflix.csv")

    movieDf2.show()
    movieDf2.printSchema()

    // manualne tworzenie DataFrame
    val row = Row("Zosia", 34, "Wojtek", 56)

    val rows = Seq(("Zosia", 34, "Wojtek", 56), ("Ada", 45, "Krzysztod", 16))

    val manualDf = spark.createDataFrame(rows)
    manualDf.show()
    manualDf.printSchema()

    import spark.implicits._
    val manualDFWithImplicits = rows.toDF("Female Name", "Female Age", "Male Name", "Male Age")
    manualDf.show()
    manualDFWithImplicits.show()

    manualDf.printSchema()
    manualDFWithImplicits.printSchema()

    val schema = manualDFWithImplicits.schema
    print(schema)

    // metody pobierania kolumn
    movieDf.select("description", "runtime").show()

    val colDes = movieDf("description")
    val colRuntime = col("runtime")
    val colId = $"id"

    movieDf.select(colDes, colRuntime, colId, col("index")).show()

//    index,id,title,type,description,release_year,age_certification,runtime,imdb_id,imdb_score,imdb_votes
    val futureRelease = col("release_year") + 2000
    movieDf2.select(futureRelease).show()

    movieDf2.select("title")
      .filter(col("release_year") > col("index")).show()

  }

  // RDD (Resilient Distributed Dataset) - rozproszona kolekcja danych, bez schema’y,
  //                                       kiepska optymalizacja
  // DataFrame - rozproszona kolekcja danych która spełnia schema'e,
  //             posiada nazwane kolumny, bardzo dobra optymalizacja
  // Dataset[MyType] - jak DataFrame ale mamy type-safety
}
