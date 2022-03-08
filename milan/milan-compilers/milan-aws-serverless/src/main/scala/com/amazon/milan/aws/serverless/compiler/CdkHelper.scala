package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.compiler.scala.CodeBlock

trait CdkHelper {
  protected val typeLifter: TypeScriptLifter

  import typeLifter._

  /**
   * Generates the import statements at the top of a typescript file.
   *
   * @param imports    All the imports that need to be included.
   *                   There can be duplicates, and they can use "{cdk}" as a placeholder for the cdk package.
   * @param cdkPackage The CDK package to insert wherever the placeholder is found.
   * @return A [[CodeBlock]] containing all of the import statements, plus a base cdk import.
   */
  protected def generateImports(imports: List[String], cdkPackage: String): CodeBlock = {
    val importStatements =
      imports
        .distinct
        .map((s: String) => s.replace("{cdk}", cdkPackage))
        .sorted
        .map(CodeBlock(_))

    qc"""import cdk = require($cdkPackage);
        |import { Construct } from "constructs";
        |//$importStatements
        |"""
  }
}
