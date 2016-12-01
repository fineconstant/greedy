package org.kduda.greedy.sample

import org.kduda.greedy.GenericUnitTest


class AssertionsSpec extends GenericUnitTest {
  "left" should "be equal to right" in {
    val left = 2
    val right = 2
    assert(left == right)
  }

  "2 + 3" must "be equal to 5" in {
    assertResult(5, "THIS IS THE CLUE") {
      2 + 3
    }
  }

  it should "succeed" in succeed

  it should "throw an IndexOutOfBoundsException" in {
    val s = "hi"
    assertThrows[IndexOutOfBoundsException] {
      s.charAt(-1)
    }
  }

//  "left " should " NOT be equal to right and fail" in {
//    val left = "abcde"
//    val right = "abcf"
//    assert(left === right)
//  }
//
//  "it" should "fail" in {
//    val left = 1
//    val right = 2
//    assert(left == right)
//  }
}