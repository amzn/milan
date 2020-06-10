package com.amazon.milan.tools

import java.io.OutputStream

import com.amazon.milan.application.ApplicationInstance


/**
 * Interface for compiling application instances.
 */
trait ApplicationInstanceCompiler {
  def compile(applicationInstance: ApplicationInstance,
              params: List[(String, String)],
              output: OutputStream): Unit
}
