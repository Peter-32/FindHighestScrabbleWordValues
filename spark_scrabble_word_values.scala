package projects

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Peter on 5/3/2016.
  */
object b_scrabble_values {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(conf)

    def scrabbleWordValue(word: String): (Int, String) = {
      // Purpose : Take a word and output the word value associated to it
      val lowerWord = word.toLowerCase()

      def letterValue(letter: String): Int = {
        // Purpose : Find a letter's value in scrabble
        letter match {
          case "a" | "e" | "i" | "o" | "u" | "l" | "n" | "s" | "t" | "r" => 1
          case "d" | "g" => 2
          case "b" | "c" | "m" | "p" => 3
          case "f" | "h" | "v" | "w" | "y" => 4
          case "k" => 5
          case "j" | "x" => 8
          case "q" | "z" => 10
        }
      }
      def scrabbleWordValueIter(lettersLeft: String, acc: Int): Int = {
        // Purpose : Iterate through each letter in order to find the total value of the word
        val firstLetter = lettersLeft.substring(0, 1)
        if (lettersLeft.length > 1) scrabbleWordValueIter(lettersLeft.drop(1), acc + letterValue(firstLetter))
        else acc + letterValue(firstLetter)
      }
      (scrabbleWordValueIter(lowerWord, 0), word)
    }

    val words = sc.textFile("dictionary.txt")
    val pairRDD = words.map(scrabbleWordValue).sortByKey(false, 1)

    pairRDD.saveAsTextFile("scrabbleValues")

  }
}
