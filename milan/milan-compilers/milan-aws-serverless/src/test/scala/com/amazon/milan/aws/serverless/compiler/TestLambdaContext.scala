package com.amazon.milan.aws.serverless.compiler

import com.amazonaws.services.lambda.runtime.{ClientContext, CognitoIdentity, Context, LambdaLogger}


class TestLambdaContext extends Context {
  val logger: LambdaLogger = new LambdaLogger {
    override def log(s: String): Unit =
      Console.out.print(s)

    override def log(bytes: Array[Byte]): Unit =
      ()
  }

  override def getAwsRequestId: String =
    "testRequestId"

  override def getLogGroupName: String =
    throw new NotImplementedError()

  override def getLogStreamName: String =
    throw new NotImplementedError()

  override def getFunctionName: String =
    throw new NotImplementedError()

  override def getFunctionVersion: String =
    throw new NotImplementedError()

  override def getInvokedFunctionArn: String =
    throw new NotImplementedError()

  override def getIdentity: CognitoIdentity =
    throw new NotImplementedError()

  override def getClientContext: ClientContext =
    throw new NotImplementedError()

  override def getRemainingTimeInMillis: Int =
    throw new NotImplementedError()

  override def getMemoryLimitInMB: Int =
    throw new NotImplementedError()

  override def getLogger: LambdaLogger =
    this.logger
}
