package com.amazon.milan.manage

import java.nio.file.Path

import com.amazon.milan.SemanticVersion
import com.amazon.milan.application.{Application, ApplicationConfiguration}
import com.amazon.milan.storage._


object ApplicationRepository {
  /**
   * Creates an [[ApplicationRepository]] that stores all objects in memory.
   *
   * @return An [[ApplicationRepository]] interface to the in-memory repository.
   */
  def createMemoryApplicationRepository(): ApplicationRepository = {
    val entityStoreFactory = new MemoryEntityStoreFactory()
    new EntityStoreApplicationRepository(entityStoreFactory)
  }

  /**
   * Creates an [[ApplicationRepository]] that stores object on the local file system.
   *
   * @param folder The root folder of the repository.
   * @return An [[ApplicationRepository]] interface for the folder.
   */
  def createLocalFolderApplicationRepository(folder: Path): ApplicationRepository = {
    val entityStoreFactory = new LocalFolderEntityStoreFactory(folder)
    new EntityStoreApplicationRepository(entityStoreFactory)
  }
}


/**
 * Interface to an application repository.
 */
trait ApplicationRepository extends Serializable {
  /**
   * Registers a new application.
   *
   * @param applicationRegistration The application registration.
   */
  def registerApplication(applicationRegistration: ApplicationRegistration): Unit

  /**
   * Gets the [[ApplicationRegistration]] record for an application.
   *
   * @param applicationId An application ID.
   * @return The [[ApplicationRegistration]] corresponding to the specified application ID.
   */
  def getApplicationRegistration(applicationId: String): ApplicationRegistration

  /**
   * Registers a new application version.
   * An application version with the same application ID and version number must not already exist in the repository.
   *
   * @param applicationVersionRegistration The application version registration.
   * @param application                    The [[Application]] corresponding to this version.
   */
  def registerVersion(applicationVersionRegistration: ApplicationVersionRegistration,
                      application: Application): Unit

  /**
   * Gets an application version registration.
   *
   * @param applicationVersionId An application version ID.
   * @return The [[ApplicationVersionRegistration]] corresponding to the ID.
   */
  def getVersionRegistration(applicationVersionId: String): ApplicationVersionRegistration

  /**
   * Registers a new application instance definition.
   *
   * @param applicationInstanceDefinitionRegistration The application instance definition registration.
   * @param configuration                             The [[ApplicationConfiguration]] corresponding to the instance definition.
   */
  def registerInstanceDefinition(applicationInstanceDefinitionRegistration: ApplicationInstanceDefinitionRegistration,
                                 configuration: ApplicationConfiguration): Unit

  /**
   * Gets an instance definition registration.
   *
   * @param instanceDefinitionId An instance definition ID.
   * @return The [[ApplicationInstanceDefinitionRegistration]] corresponding to the ID.
   */
  def getInstanceDefinitionRegistration(instanceDefinitionId: String): ApplicationInstanceDefinitionRegistration

  /**
   * Gets all instance definitions using an application version.
   *
   * @param applicationVersionId An application version ID.
   * @return All instance definitions using the specified version.
   */
  def listInstanceDefinitions(applicationVersionId: String): List[ApplicationInstanceDefinitionRegistration]

  /**
   * Registers an application package.
   *
   * @param applicationPackageRegistration The application package registration.
   */
  def registerPackage(applicationPackageRegistration: ApplicationPackageRegistration): Unit

  /**
   * Gets an application package registration.
   *
   * @param applicationPackageId A package ID.
   * @return The [[ApplicationPackageRegistration]] corresponding to the package ID.
   */
  def getPackageRegistration(applicationPackageId: String): ApplicationPackageRegistration

  /**
   * Registers an application instance.
   *
   * @param applicationInstanceRegistration The application instance registration.
   */
  def registerInstance(applicationInstanceRegistration: ApplicationInstanceRegistration): Unit

  /**
   * Gets an application instance registration.
   *
   * @param applicationInstanceId An applicaiton instance ID.
   * @return The [[ApplicationInstanceRegistration]] associated with the ID.
   */
  def getInstanceRegistration(applicationInstanceId: String): ApplicationInstanceRegistration

  /**
   * Gets all instance registrations using an instance definition.
   *
   * @param instanceDefinitionId An application instance definition ID.
   * @return All instances using the specified definition ID.
   */
  def listInstances(instanceDefinitionId: String): List[ApplicationInstanceRegistration]

  /**
   * Gets whether an application with the specified ID exists in the repository.
   *
   * @param applicationId An application ID.
   * @return A boolean value indicating whether the application exists.
   */
  def applicationExists(applicationId: String): Boolean

  /**
   * Gets whether an application version exists in the repository.
   *
   * @param applicationId An application ID.
   * @param version       A version.
   * @return A boolean value indicating whether a registration exists for the specified version of the application.
   */
  def versionExists(applicationId: String, version: SemanticVersion): Boolean

  /**
   * Gets the latest version of an application.
   *
   * @param applicationId An application ID.
   * @return The [[ApplicationVersionRegistration]] corresponding to the latest version of the application.
   */
  def getLatestVersion(applicationId: String): ApplicationVersionRegistration

  /**
   * Gets all versions of an application.
   *
   * @param applicationId An application ID.
   * @return A [[TraversableOnce]] that yields all registered versions of the application.
   */
  def listAllVersions(applicationId: String): TraversableOnce[ApplicationVersionRegistration]

  /**
   * Gets the [[Application]] corresponding to a specified version of an application.
   *
   * @param applicationId An application ID.
   * @param version       A an application version.
   * @return The [[Application]] corresponding to the specified application and version.
   */
  def getApplicationDefinition(applicationId: String, version: SemanticVersion): Application

  /**
   * Gets the [[ApplicationConfiguration]] corresponding to a specified instance definition of an application.
   *
   * @param applicationId        An application ID.
   * @param instanceDefinitionId An application instance definition ID.
   * @return The [[ApplicationConfiguration]] corresponding to the specified application and instance definition.
   */
  def getApplicationConfiguration(applicationId: String, instanceDefinitionId: String): ApplicationConfiguration
}
