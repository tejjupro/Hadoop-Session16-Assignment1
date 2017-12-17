/*
 * This program implements operation on List
 * 
 *
  Problem statement:
  Given a list of numbers - List[Int] (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
- find the sum of all numbers
- find the total elements in the list
- calculate the average of the numbers in the list
- find the sum of all the even numbers in the list
- find the total number of elements in the list divisible by both 5 and 3


 * 
 */


package assignment.listDemo

import org.apache.spark.SparkConf
//import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ListDemo {
  def main(args: Array[String]): Unit = {
    //creating an instance of SparkConf to provide the spark configurations.This will make spark to run in local mode
    val conf = new SparkConf().setAppName("Working with List ").setMaster("local")
    
    //Providing configuration parameter to SparkContext with an  instance of SparkConf
    val sc = new SparkContext(conf)
    
    //Using parallelize to make the scala collection ,list An RDD
    val list = sc.parallelize(List[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    
    //sum is an RDD action of numberic type ,which returns the sum of elements in the RDD list
    val sumRDD = list.sum()
    //count is an RDD which counts the number of elements present in the RDD list
    val countElementRDD = list.count()

    //mean in rdd which counts the average of elements present in the RDD list
    val avgRdd = list.mean()
    
    //filter is an RDD which filters the RDD based on condition that element of rdd is even and get the sum of those elements 
     val sumEvenRDD = list.filter(num => num % 2 == 0).sum
  
    /*filter is an RDD which filters the RDD based on condition that element of rdd is divisible by both 5 and 3 
    and get the sum of those elsements */
     val divisibleEleRDD = list.filter(num => num % 5 == 0 && num % 3 == 0).count
    
    println("Elements present in the list are: ")
    //Printing the elements in the list 
    list.foreach(println)
    //Printing all the output in the console
    println("Sum of all numbers :" + sumRDD)

    println("Total elements in the list :" + countElementRDD)

    println("Average of the numbers in the list :" + avgRdd)

    println("Sum of all the even numbers in the list :" + sumEvenRDD)

    println("Total number of elements in the list divisible by both 5 and 3 :" + divisibleEleRDD)

  }
}



