package com.amazon.milan.aws.metrics

/**
 * An object that can generate JSON for a CloudFormation element.
 */
trait CloudFormationJsonGenerator {

  /**
   * @return CloudFormation JSON.
   */
  def toJson: String
}
