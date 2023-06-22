package com.de.app

import org.scalatest.FunSuite

class SumCalculatorTest extends  FunSuite{

  //TODO: Correct the assertion logic to pass the test
  test("To test Sum Calculator"){
    assert(SumCalculator.sum(10,15) == 24)
  }

}
