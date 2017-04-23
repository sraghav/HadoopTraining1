import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by raghav on 2017-04-22.
  */
object TestClass {

  def main(args: Array[String]): Unit = {


    val sconf = new SparkConf()
    .setMaster("spark://raghavmacbookpro:7077")
    .setAppName("My Test App")
    //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //.setSparkHome("/hadoop/spark")
    //.set("spark.local.ip","127.0.0.1")

    val sc = new SparkContext(sconf)

    sc.setLogLevel("INFO")

    println("Hello Raghav")

    val myList = List (1,2,3,4,5,6,7,8,9)

    val myRDD = sc.parallelize(myList,4)

    val sqRDD = myRDD.map(elem => {
      elem * elem
    })


//
//    println("printing now")
//    sqRDD.collect.foreach(println)
//
//
//    val nm= List("ra","gh","v")
//
//    val x = sc.parallelize(
//      List(1,2,3,4,5,6,7,8,9)
//      ,5)
//
//    val x1 = sc.parallelize(
//      List("ra","gh","v",1,5)
//      ,5)
//
//    //val x2 = sc.makeRDD(Seq(x1))

//    val y = x.mapPartitionsWithIndex{
//      (idx, itr) =>
//      {
//        itr.map(x => x + " square => "+x*x+" lives in partition " + idx)
//
//      }
//    }
//
//    y.collect.foreach(println)
//
//    val cnt = y.count
//    println("Y contains "+cnt)



    val file=sc.textFile("hdfs://raghavmacbookpro:9000/user/raghav/ShakesPeareWorks.txt",5)
    val wc = file.flatMap(_.split(" ")).
      filter(x=>{x.length > 3}).
      map(x=> x.toLowerCase).
      map(x => {(x,1)}).
      reduceByKey(_+_).
      map({case (w,cnt) => (cnt, w)} ).
      sortByKey(false)


  }

}
