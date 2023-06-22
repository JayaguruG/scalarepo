package com.de.app

import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}

class SquareCalculatorTest extends FeatureSpec with GivenWhenThen with Matchers{


  //TODO: Change ignore() to scenario() to enable the test
  feature("User can calculate square using Square Calculator") {
    ignore("A square function is invoked for Square Calculator") {
      Given("SquareCalculator square implementation")
      When("SqaureCalculator.square in invoked")
      val number = 3
      Then("The Square of the number should be returned")
      assert(SquareCalculator.square(number) === 9)
    }
  }

}
