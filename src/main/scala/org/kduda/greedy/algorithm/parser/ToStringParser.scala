package org.kduda.greedy.algorithm.parser

object ToStringParser {

  /**
    * Creates String in a .rul RSES format ready to be saved in a file or DB.
    *
    * @param dtsRules Decision rules to be parsed.
    * @return String containing rules in .rul RSES format.
    */
  def buildStringRSES(dtsRules: Map[String, List[List[(String, String)]]]): String = {
    val builder = StringBuilder.newBuilder

    for (dtRules <- dtsRules.values) {
      for (rule <- dtRules) {
        val support = rule.head._2
        val decision = rule.tail.head
        val conditions = rule.tail.tail

        val formatted = conditions.map {
          case (attr, value) =>
            s"($attr=$value)"
        }

        builder.append(formatted.mkString("&"))

        builder.append("=>")
        builder.append(s"(${decision._1}=${decision._2}[$support])")
        builder.append(s" $support\n")
      }
    }
    builder.mkString
  }
}
