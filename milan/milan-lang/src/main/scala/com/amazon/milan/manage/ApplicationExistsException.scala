package com.amazon.milan.manage


class ApplicationExistsException(val applicationId: String) extends Exception(s"An application with ID '$applicationId' already exists.") {
}
