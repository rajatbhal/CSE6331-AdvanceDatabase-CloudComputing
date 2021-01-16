import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer

object Partition {
 
  val depth = 6

  def main ( args: Array[ String ] ) {
    val conf = new SparkConf().setAppName("GraphPartition")
    val sc = new SparkContext(conf)
    var temp = 0

    var graph = sc.textFile(args(0)).map( line => { val a = line.split(",")
    var nodeID : Long = a(0).toLong
    var clusterID : Long = -1
    var adjacent = ListBuffer[Long]()

    if(temp < 5){
      clusterID = nodeID
      temp += 1
    }

    for(i <- 1 to a.length-1){ adjacent += a(i).toLong }
    (nodeID, clusterID, adjacent)

      }) /* read graph from args(0); the graph cluster ID is set to -1 except for the first 5 nodes */

for (i <- 1 to depth){
   graph = graph.flatMap{ case(nodeID, clusterID, adjacent)=> (nodeID, clusterID); adjacent.map(p =>(p, clusterID)) }
                .reduceByKey(_ max _)
                .join( graph.map( l => (l._1,(l._2,l._3)) ))
                .map{ l =>
                  if(l._2._2._1 >0){
                    (l._1, l._2._2._1, l._2._2._2)
                  }else{
                    (l._1, l._2._1, l._2._2._2)
                  }

                  }
                  }

/* finally, print the partition sizes */
graph.map(l=>(l._2,1)).reduceByKey(_ + _).sortBy(_._1).collect().foreach(println)


  }
}
          