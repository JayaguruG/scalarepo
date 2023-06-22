package com.de.app

import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}

class CubeCalculatorTest extends  FeatureSpec with GivenWhenThen with Matchers {


  feature("User can calculate cube using Cube Calculator") {
    scenario("A cube function is invoked for Cube Calculator") {
      Given("CubeCalculator cube implementation")
      When("CubeCalculator.cube in invoked")
      val number = 3
      Then("The Cube of the number should be returned")
      assert(CubeCalculator.cube(number) === 27)
    }
  }

}
