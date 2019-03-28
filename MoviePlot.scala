import org.apache.spark.{SparkConf, SparkContext}

object MoviePlot{
  def main(args: Array[String]): Unit ={
    //System.setProperty("hadoop.home.dir", "/usr/local/Cellar/hadoop/3.1.1")
    val sc = new SparkContext(new SparkConf().setAppName("Part1_MoviePlot"))
    //val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Part1_MoviePlot"))
    val searchTerm = args(1)
    //val searchTerm = "life learning"
    val folderPath = args(0)
    //val folderPath = "./"
    val search = searchTerm.toLowerCase().split(" ")

    val movieMetaData = sc.textFile(folderPath + "movie.metadata.tsv")
    val movieKeyValue = sc.textFile(folderPath + "plot_summaries.txt")
    val stopWords = sc.textFile(folderPath + "stopwords.txt")

    val movieMap = movieMetaData.map(x => (x.split("\t")(0), x.split("\t")(2)))
    val stopWordsSet = stopWords.flatMap(x => x.split(",")).collect().toSet
    val removedPunct = movieKeyValue.map(x => x.replaceAll("""[\p{Punct}]""", " "))
    val filteredLine = removedPunct.map(x => x.toLowerCase.split("""\s+"""))
    val output = filteredLine.map(x => x.map(y => y).filter(word => stopWordsSet.contains(word) == false))


    val docID = movieKeyValue.map(_.split("\t"))
    val countOfDocuments = docID.map(x => (x(0), 1)).reduceByKey((a, b) => a + b).count()

    def tf(y: String)= output.map(x => (x(0), x.count(_.contains(y)).toDouble/x.size)).filter(x=>x._2!=0.0)
    val TF = search.map(y => tf(y).collect().toMap)

    def DFfunction(x: String) = movieKeyValue.flatMap(y => y.split("\n").filter(t => t.contains(x))).map(y => ("t", 1)).reduceByKey(_ + _).collect()(0)._2
    val DF = search.map(y => DFfunction(y))

    val IDF = DF.map(y => (1+ math.log(countOfDocuments/y)))

    def TFIDFfunction(x: Int) = TF(x).map(a=>(a._1,a._2*IDF(x))).toMap
    val TF_IDF = TF.zipWithIndex.map{ case (e, i) =>TFIDFfunction(i) }

    val TFQuery =  search.map(y => search.count(_.contains(y)).toDouble/search.size)
    val TF_IDFQuery = TFQuery.zipWithIndex.map{case (e, i) => e * IDF(i)}
    val query = math.sqrt(TF_IDFQuery.reduce((x,y) => x *x + y *y))
    val distinctIDs = TF_IDF.flatMap(x => x.map(y=>y._1)).toList.distinct.toArray

    def documentFunction(x:String)= search.zipWithIndex.map{case (e, i) => (TF_IDF(i).get(x).getOrElse(0.0).asInstanceOf[Double]).toDouble }.reduce((x,y)=>x*x+y*y)
    val document = distinctIDs.map(x =>  (x, math.sqrt(documentFunction(x) ))).toMap

    def dotFunction(x:String)= search.zipWithIndex.map{case (e, i) => (TF_IDFQuery(i) * TF_IDF(i).get(x).getOrElse(0.0).asInstanceOf[Double]).toDouble }.reduce((x,y)=>x+y)

    val dotProduct = distinctIDs.map(x =>  (x, dotFunction(x))).toMap
    val cosine = distinctIDs.map( x=> (x, dotProduct.get(x).getOrElse(0.0).asInstanceOf[Double] / (document.get(x).getOrElse(0.0).asInstanceOf[Double] * query)))
    val cosine1= sc.parallelize(cosine)
    val finalresult = movieMap.join(cosine1).map(x=>(x._2._1,x._2._2)).sortBy(_._2).map(_._1).take(10)
    val finalRDD = sc.parallelize(finalresult)
    val finalOutput = finalRDD.saveAsTextFile(folderPath + "MoviePlot_Output")
  }
}
