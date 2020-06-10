package com.amazon.milan.tools

import com.amazon.milan.application.ApplicationInstance


trait ApplicationInstanceProvider {
  /**
   * Gets an application instance.
   *
   * @param params User-supplied parameters.
   * @return An [[ApplicationInstance]].
   */
  def getApplicationInstance(params: List[(String, String)]): ApplicationInstance
}
