import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math._

object KMeans {
  type Point = (Double,Double)

  var centroids: Array[Point] = Array[Point]()

  def distance(p1 : Point, p2 : Point) : Double =
  	Math.sqrt(pow(p2._2 - p1._2, 2) + pow(p2._1 - p1._1, 2))
  

  def main(args: Array[ String ]) {
    val conf = new SparkConf().setAppName("KMeansClustering")
    val sc = new SparkContext(conf)

    centroids = sc.textFile(args(1)).map( line => { val a1 = line.split(","); (a1(0).toDouble,a1(1).toDouble) } ).collect

    var points = sc.textFile(args(0)).map( line => { val a2 = line.split(","); (a2(0).toDouble,a2(1).toDouble) } )

    for ( i <- 1 to 5 ){
    val cs = sc.broadcast(centroids)
        centroids = points.map{ p => (cs.value.minBy(distance(p,_)), p)}
        					.groupByKey().map{ case(c, centroidpts)=>

        var count:Int = 0
        var xtotal = 0.0
        var ytotal = 0.0

        for(p <- centroidpts) {
           count += 1
           xtotal = xtotal + p._1
           ytotal = ytotal + p._2
        	}
        
        var x = xtotal/count
        var y = ytotal/count
        (x, y)

      }.collect
}
    

    centroids.foreach(println)
  }
}