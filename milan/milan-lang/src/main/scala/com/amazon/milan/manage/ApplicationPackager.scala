package com.amazon.milan.manage

import com.amazon.milan.application.ApplicationInstance


trait ApplicationPackager {
  /**
   * Creates a package containing a serialized [[ApplicationInstance]] as well as all of the functionality necessary to run
   * the application.
   *
   * @param instanceDefinition The application instance definition to package.
   * @return The ID of the application package that was created.
   */
  def packageApplication(instanceDefinition: ApplicationInstance): String
}
