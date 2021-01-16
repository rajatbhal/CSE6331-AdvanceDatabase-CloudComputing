import org.apache.spark.graphx.{Graph, VertexId,Edge}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf



object Partition {
  def main ( args: Array[String] ) {
  	val conf = new SparkConf().setAppName("GraphPartition")
    val sc = new SparkContext(conf)

     var temp:Long = 0

    val edges = sc.textFile(args(0)).map( line => { val a = line.split(",").map(_.toLong)
    
         temp += 1
         (a(0),a.tail.toList,temp) } ).flatMap( x => x._2.map(y => (x._1, y,x._3)))
     .map(nodes =>
     {
	     Edge(nodes._1, nodes._2, nodes._3)
     }
	
	)


	val graph = Graph.fromEdges(edges,1L)

	val groupa = graph.edges.filter(e => e.attr <= 5).map(e=>{e.srcId}).distinct().collect()
	val element = sc.broadcast(groupa)
	val extendedgraph = graph.mapVertices((id,_) => {
      if(element.value.contains(id))
        id
      else
        -1L
    })

	val g = extendedgraph.pregel(-1L,maxIterations = 6)(
      (_,attr,newAttr)=> {
        if(attr == -1L)
          newAttr
        else
          attr
      },
      triplet => {
        Iterator((triplet.dstId,triplet.srcAttr))
      },
      (a,b)=> Math.max(a,b)
    )
        val res =g.vertices.map(graph=>(graph._2,1)).reduceByKey(_+_) 
        
        res.collect().foreach(println)
  }
}