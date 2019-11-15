package com.amazon.milan.manage

import com.amazon.milan.SemanticVersion
import com.amazon.milan.application.{Application, ApplicationConfiguration}
import com.amazon.milan.storage._
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


/**
 * An implementation of [[ApplicationRepository]] that uses an [[EntityStore]] interface to store
 * repository records.
 *
 * @param entityStoreFactory An [[EntityStoreFactory]] that will be used to store application repository entities.
 */
class EntityStoreApplicationRepository(entityStoreFactory: EntityStoreFactory)
  extends ApplicationRepository {

  private val VERSIONS_FOLDER = "versions"
  private val VERSIONS_BY_ID_FOLDER = "versionsbyid"
  private val INSTANCE_DEFINITIONS_FOLDER = "instancedefs"
  private val INSTANCE_DEFINITION_BY_ID_FOLDER = "instancedefsbyid"
  private val INSTANCE_DEFINITION_BY_VERSION_FOLDER = "instancedefsbyversionid"
  private val PACKAGES_FOLDER = "packages"
  private val PACKAGES_BY_ID_FOLDER = "packagesbyid"
  private val INSTANCES_FOLDER = "instances"
  private val INSTANCES_BY_ID_FOLDER = "instancesbyid"
  private val INSTANCES_BY_DEFINITION_FOLDER = "instancesbydefid"

  private val APP_DEFINITIONS_FOLDER = "appdefs"
  private val APP_CONFIGS_FOLDER = "appconfigs"

  private val logger = Logger(LoggerFactory.getLogger(getClass))

  override def registerApplication(applicationRegistration: ApplicationRegistration): Unit = {
    if (this.applicationExists(applicationRegistration.applicationId)) {
      throw new ApplicationExistsException(applicationRegistration.applicationId)
    }

    this.getApplicationStore(applicationRegistration.applicationId)
      .putEntity(applicationRegistration.applicationId, applicationRegistration)
  }

  override def getApplicationRegistration(applicationId: String): ApplicationRegistration = {
    try {
      this.getApplicationStore(applicationId).getEntity(applicationId)
    }
    catch {
      case ex: EntityNotFoundException =>
        throw new ApplicationNotFoundException(applicationId, ex)
    }
  }

  override def registerVersion(applicationVersionRegistration: ApplicationVersionRegistration,
                               application: Application): Unit = {
    val applicationId = applicationVersionRegistration.applicationId

    this.validateApplicationExists(applicationId)

    if (this.versionExists(applicationId, applicationVersionRegistration.version)) {
      throw new ApplicationVersionExistsException(applicationVersionRegistration.applicationId, applicationVersionRegistration.version)
    }

    val versionKey = this.getVersionKey(applicationVersionRegistration.version)

    this.getVersionStore(applicationId).putEntity(versionKey, applicationVersionRegistration)

    // Also store the version by ApplicationVersionId because sometimes we need to look it up based on that.
    this.getVersionByIdStore.putEntity(applicationVersionRegistration.applicationVersionId, applicationVersionRegistration)

    // Write the application definition object.
    this.getApplicationDefinitionStore(applicationId).putEntity(versionKey, application)
  }

  override def getVersionRegistration(applicationVersionId: String): ApplicationVersionRegistration = {
    try {
      this.getVersionByIdStore.getEntity(applicationVersionId)
    }
    catch {
      case ex: EntityNotFoundException =>
        throw new ApplicationVersionNotFoundException(applicationVersionId, ex)
    }
  }

  override def registerInstanceDefinition(applicationInstanceDefinitionRegistration: ApplicationInstanceDefinitionRegistration,
                                          configuration: ApplicationConfiguration): Unit = {
    val applicationId = applicationInstanceDefinitionRegistration.applicationId
    val instanceDefinitionId = applicationInstanceDefinitionRegistration.instanceDefinitionId

    this.validateVersionExists(applicationId, applicationInstanceDefinitionRegistration.applicationVersionId)

    this.getInstanceDefinitionStore(applicationId)
      .putEntity(instanceDefinitionId, applicationInstanceDefinitionRegistration)

    this.getInstanceDefinitionByIdStore
      .putEntity(instanceDefinitionId, applicationInstanceDefinitionRegistration)

    // Write the app config object.
    this.getAppConfigStore(applicationId).putEntity(instanceDefinitionId, configuration)
  }

  override def getInstanceDefinitionRegistration(instanceDefinitionId: String): ApplicationInstanceDefinitionRegistration = {
    try {
      this.getInstanceDefinitionByIdStore.getEntity(instanceDefinitionId)
    }
    catch {
      case ex: EntityNotFoundException =>
        throw new ApplicationInstanceDefinitionNotFoundException(instanceDefinitionId, ex)
    }
  }

  override def listInstanceDefinitions(applicationVersionId: String): List[ApplicationInstanceDefinitionRegistration] = {
    this.getInstanceDefinitionByVersionStore(applicationVersionId).listEntities().toList
  }

  override def registerPackage(applicationPackageRegistration: ApplicationPackageRegistration): Unit = {
    this.validateInstanceDefinitionExists(applicationPackageRegistration.applicationId, applicationPackageRegistration.instanceDefinitionId)

    this.getPackageStore(applicationPackageRegistration.applicationId)
      .putEntity(applicationPackageRegistration.applicationPackageId, applicationPackageRegistration)

    this.getPackageByIdStore.putEntity(applicationPackageRegistration.applicationPackageId, applicationPackageRegistration)
  }

  override def getPackageRegistration(applicationPackageId: String): ApplicationPackageRegistration = {
    try {
      this.getPackageByIdStore.getEntity(applicationPackageId)
    }
    catch {
      case ex: EntityNotFoundException =>
        throw new ApplicationPackageNotFoundException(applicationPackageId, ex)
    }
  }

  override def registerInstance(applicationInstanceRegistration: ApplicationInstanceRegistration): Unit = {
    this.logger.info(s"Registering application instance $applicationInstanceRegistration.")

    this.getInstanceStore(applicationInstanceRegistration.applicationId)
      .putEntity(applicationInstanceRegistration.applicationInstanceId, applicationInstanceRegistration)

    this.getInstanceByIdStore.putEntity(applicationInstanceRegistration.applicationInstanceId, applicationInstanceRegistration)

    this.getInstanceByDefinitionIdStore(applicationInstanceRegistration.instanceDefinitionId)
      .putEntity(applicationInstanceRegistration.applicationInstanceId, applicationInstanceRegistration)
  }

  override def getInstanceRegistration(applicationInstanceId: String): ApplicationInstanceRegistration = {
    try {
      this.getInstanceByIdStore.getEntity(applicationInstanceId)
    }
    catch {
      case ex: EntityNotFoundException =>
        throw new ApplicationInstanceNotFoundException(applicationInstanceId, ex)
    }
  }

  override def listInstances(instanceDefinitionId: String): List[ApplicationInstanceRegistration] = {
    this.getInstanceByDefinitionIdStore(instanceDefinitionId).listEntities().toList
  }

  override def applicationExists(applicationId: String): Boolean = {
    this.getApplicationStore(applicationId).entityExists(applicationId)
  }

  override def versionExists(applicationId: String, version: SemanticVersion): Boolean = {
    this.validateApplicationExists(applicationId)

    val versionKey = this.getVersionKey(version)
    this.getVersionStore(applicationId).entityExists(versionKey)
  }

  override def getLatestVersion(applicationId: String): ApplicationVersionRegistration = {
    val versionStore = this.getVersionStore(applicationId)
    val versions = versionStore.listEntityKeys().map(this.parseVersionKey).toList

    if (versions.isEmpty) {
      throw new NoApplicationVersionsException(applicationId)
    }

    val latestVersion = versions.max

    // This is a race condition if deleting application versions ever becomes allowed.
    val versionKey = this.getVersionKey(latestVersion)
    versionStore.getEntity(versionKey)
  }

  override def listAllVersions(applicationId: String): TraversableOnce[ApplicationVersionRegistration] = {
    this.getVersionStore(applicationId).listEntities()
  }

  override def getApplicationDefinition(applicationId: String, version: SemanticVersion): Application = {
    this.validateVersionExists(applicationId, version)

    val versionKey = this.getVersionKey(version)
    this.getApplicationDefinitionStore(applicationId).getEntity(versionKey)
  }

  override def getApplicationConfiguration(applicationId: String, instanceDefinitionId: String): ApplicationConfiguration = {
    this.getAppConfigStore(applicationId).getEntity(instanceDefinitionId)
  }

  def instanceDefinitionExists(applicationId: String, instanceDefinitionId: String): Boolean = {
    this.getInstanceDefinitionStore(applicationId).entityExists(instanceDefinitionId)
  }

  def versionExists(versionId: String): Boolean = {
    this.getVersionByIdStore.entityExists(versionId)
  }

  private def validateApplicationExists(applicationId: String): Unit = {
    if (!this.applicationExists(applicationId)) {
      throw new ApplicationNotFoundException(applicationId)
    }
  }

  private def validateVersionExists(applicationId: String, version: SemanticVersion): Unit = {
    this.validateApplicationExists(applicationId)

    if (!this.versionExists(applicationId, version)) {
      throw new ApplicationVersionNotFoundException(applicationId, version)
    }
  }

  private def validateVersionExists(applicationId: String, versionId: String): Unit = {
    this.validateApplicationExists(applicationId)

    if (!this.versionExists(versionId)) {
      throw new ApplicationVersionNotFoundException(applicationId, versionId)
    }
  }

  private def validateInstanceDefinitionExists(applicationId: String, instanceDefinitionId: String): Unit = {
    this.validateApplicationExists(applicationId)

    if (!this.instanceDefinitionExists(applicationId, instanceDefinitionId)) {
      throw new ApplicationInstanceDefinitionNotFoundException(applicationId, instanceDefinitionId)
    }
  }

  private def getVersionKey(version: SemanticVersion): String = {
    version.withoutBuildMetadata().toString
  }

  private def parseVersionKey(versionKey: String): SemanticVersion = {
    SemanticVersion.parse(versionKey)
  }

  private def getApplicationStore(applicationId: String): EntityStore[ApplicationRegistration] = {
    this.entityStoreFactory.createEntityStore[ApplicationRegistration](applicationId)
  }

  private def getVersionStore(applicationId: String): EntityStore[ApplicationVersionRegistration] = {
    this.entityStoreFactory.createEntityStore[ApplicationVersionRegistration](applicationId + "/" + VERSIONS_FOLDER)
  }

  private def getVersionByIdStore =
    this.entityStoreFactory.createEntityStore[ApplicationVersionRegistration](VERSIONS_BY_ID_FOLDER)

  private def getApplicationDefinitionStore(applicationId: String): EntityStore[Application] = {
    this.entityStoreFactory.createEntityStore[Application](applicationId + "/" + APP_DEFINITIONS_FOLDER)
  }

  private def getPackageStore(applicationId: String): EntityStore[ApplicationPackageRegistration] = {
    this.entityStoreFactory.createEntityStore[ApplicationPackageRegistration](applicationId + "/" + PACKAGES_FOLDER)
  }

  private def getPackageByIdStore =
    this.entityStoreFactory.createEntityStore[ApplicationPackageRegistration](PACKAGES_BY_ID_FOLDER)

  private def getAppConfigStore(applicationId: String): EntityStore[ApplicationConfiguration] = {
    this.entityStoreFactory.createEntityStore[ApplicationConfiguration](applicationId + "/" + APP_CONFIGS_FOLDER)
  }

  private def getInstanceDefinitionStore(applicationId: String): EntityStore[ApplicationInstanceDefinitionRegistration] = {
    this.entityStoreFactory.createEntityStore[ApplicationInstanceDefinitionRegistration](applicationId + "/" + INSTANCE_DEFINITIONS_FOLDER)
  }

  private def getInstanceDefinitionByVersionStore(applicationVersionId: String): EntityStore[ApplicationInstanceDefinitionRegistration] = {
    this.entityStoreFactory.createEntityStore[ApplicationInstanceDefinitionRegistration](INSTANCE_DEFINITION_BY_VERSION_FOLDER + "/" + applicationVersionId)
  }

  private def getInstanceDefinitionByIdStore =
    this.entityStoreFactory.createEntityStore[ApplicationInstanceDefinitionRegistration](INSTANCE_DEFINITION_BY_ID_FOLDER)

  private def getInstanceStore(applicationId: String): EntityStore[ApplicationInstanceRegistration] = {
    this.entityStoreFactory.createEntityStore[ApplicationInstanceRegistration](applicationId + "/" + INSTANCES_FOLDER)
  }

  private def getInstanceByIdStore =
    this.entityStoreFactory.createEntityStore[ApplicationInstanceRegistration](INSTANCES_BY_ID_FOLDER)

  private def getInstanceByDefinitionIdStore(instanceDefinitionId: String): EntityStore[ApplicationInstanceRegistration] = {
    this.entityStoreFactory.createEntityStore[ApplicationInstanceRegistration](INSTANCES_BY_DEFINITION_FOLDER + "/" + instanceDefinitionId)
  }
}
