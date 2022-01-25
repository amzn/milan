package com.amazon.milan.tools

import com.amazon.milan.application.ApplicationInstance


/**
 * Interface for compiling application instances.
 */
trait ApplicationInstanceCompiler {
  /**
   * Compiles an application instance.
   *
   * @param applicationInstance The application instance to compile.
   * @param params              User-provided compiler parameters.
   * @param outputs             User-provided named output paths.
   */
  def compile(applicationInstance: ApplicationInstance,
              params: InstanceParameters,
              outputs: CompilerOutputs): Unit
}
