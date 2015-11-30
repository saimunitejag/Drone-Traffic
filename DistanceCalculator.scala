import java.io.PrintWriter
import java.io.File
import scala.io
import scala.math._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object LatLong {
  def main(args: Array[String]) {
  
  val radius:Double = 6371
  
  def calculateDistance1(lonA:String,latA:String,lonB:String, latB:String ):Double = {
    
    var centralangle:Double = 0
    val phi1 = toDegree(latA)._1.toRadians
    val phi2 = toDegree(latB)._1.toRadians
    val lam1 = toDegree(lonA)._1.toRadians
    val lam2 = toDegree(lonB)._1.toRadians
    val phi12 = angleDifference(latA,latB).toRadians
    val lam12 = angleDifference(lonA, lonB).toRadians
    
    val numerator:Double = sqrt(pow(cos(phi2)*sin(lam12),2) + pow(cos(phi1)*sin(phi2) - sin(phi1)*cos(phi2)*cos(lam12),2))
    val denominator:Double = sin(phi1)*sin(phi2) + cos(phi1)*cos(phi2)*cos(lam12)
    
    centralangle = atan2(numerator,denominator)
    
    return radius*centralangle
    
  } 
  
  def calulateDistance01(lonA:Double,latA:Double,lonB:Double, latB:Double):Double = {
    var centralangle:Double = 0
    val phi1 = latA.toRadians
    val phi2 = latB.toRadians
    val lam1 = lonA.toRadians
    val lam2 = lonB.toRadians
    val phi12 = angleDifference1(latA,latB).toRadians
    val lam12 = angleDifference1(lonA, lonB).toRadians
    
    val numerator:Double = sqrt(pow(cos(phi2)*sin(lam12),2) + pow(cos(phi1)*sin(phi2) - sin(phi1)*cos(phi2)*cos(lam12),2))
    val denominator:Double = sin(phi1)*sin(phi2) + cos(phi1)*cos(phi2)*cos(lam12)
    
    centralangle = atan2(numerator,denominator)
    println(centralangle)
    
    return radius*centralangle
    
    
  }
  
  
  def calculateDistance0(lonA:String,latA:String,lonB:String, latB:String ):Double = {
    var centralangle:Double = 0
    val phi1 = toDegree(latA)._1.toRadians
    val phi2 = toDegree(latB)._1.toRadians
    val lam1 = toDegree(lonA)._1.toRadians
    val lam2 = toDegree(lonB)._1.toRadians
    val phi12 = angleDifference(latA,latB).toRadians
    val lam12 = angleDifference(lonA, lonB).toRadians
    
    centralangle = 2*asin(sqrt(pow(sin(phi12/2),2) + cos(phi1)*cos(phi2)*pow(sin(lam12/2),2)))
    
    return radius*centralangle
  }
  
  
  def angleDifference(angle1:String, angle2:String):Double = {
    
    var diffangle:Double = 0
    var angle1t = toDegree(angle1)
    var angle2t = toDegree(angle2)
    
    if(angle1t._2 == angle2t._2){
     
        diffangle = (angle1t._1 - angle2t._1).abs 
      
    } else {
      
       if( angle1t._2 =="E" || angle1t._2 == "W"){
        
        diffangle = 360 - (angle1t._1 + angle2t._1).abs 
        
      } else {
        
        diffangle = (angle1t._1 + angle2t._1).abs
        
        
      }
      
    }
    
    return diffangle

    
  }
  
  def angleDifference1(angle1:Double, angle2:Double):Double = {
    
    var diffangle:Double = 0
    
    diffangle = (angle2 - angle1).abs
    
    return diffangle
    
  }
  
  def toDegree(angle:String):Tuple2[Double,String] = {
    var degreesplit:Array[String] = angle.split("\u00B0")
    var degree:Double = degreesplit(0).toDouble
    var minsplit:Array[String] = degreesplit(1).split("'")
    var mintodegree:Double = minsplit(0).toDouble
    var secondssplit:Array[String] = minsplit(1).split(""""""")
    var secondstodegree:Double = secondssplit(0).toDouble
    var direction:String = secondssplit(1)
    
    
    degree = degree + mintodegree/60 + secondstodegree/3600
    
    val anglet = new Tuple2(degree,direction)
    
    return anglet
    
  }
  
    val longitude1:String = """121°53'10.78"W"""
    val latitude1:String = """37°20'20.08"N"""
    val longitude2:String = """121°59'20.18"W"""
    val latitude2:String = """37°32'54.73"N"""
    
    val longitude01:Double = -121.9895320893645
    val latitude01:Double = 37.54827885235077
    val longitude02:Double = -121.9884945915362
    val latitude02:Double = 37.54618145311554
    
    val conf = new SparkConf().setAppName("app1").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val csv = sc.textFile("/home/administrator/DronesStations.csv")
    val data = csv.filter(a=>a(0)!='D').map(line => line.split(",").map(elem => elem.trim))
    val lat_long = data.map(abc => (abc(2),abc(3)))
    val ll = lat_long.collect()
    val writer = new PrintWriter(new File("/home/administrator/hi_bud.csv"))
    for(i <- ll){
      for(j <- ll){
        val (a1, a2, a3, a4) = (i._1.toString().replace("\"\"", "~").replace("\"", "").replace("~","\""),i._2.toString().replace("\"\"", "~").replace("\"", "").replace("~","\""),j._1.toString().replace("\"\"", "~").replace("\"", "").replace("~","\""),j._2.toString().replace("\"\"", "~").replace("\"", "").replace("~","\""))
        println(a1,a2,a3,a4)
        val d = calculateDistance1(a1,a2,a3,a4)
        writer.write(d.toString)
				writer.write(",")
      }
    writer.write("\n")
    }
    writer.close()
    println("done")
  }
  
}
