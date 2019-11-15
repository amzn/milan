package com.amazon.milan.manage

import com.amazon.milan.SemanticVersion
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.application.{Application, ApplicationConfiguration}
import com.amazon.milan.lang.{Stream, StreamGraph}
import com.amazon.milan.storage.MemoryEntityStoreFactory
import com.amazon.milan.test.IntRecord
import org.junit.Assert._
import org.junit.Test


@Test
class TestEntityStoreApplicationRepository {
  @Test
  def test_EntityStoreApplicationRepository_ApplicationExists_WithNoApplicationsRegistered_ReturnsFalse(): Unit = {
    val store = this.createStore()

    assertFalse(store.applicationExists("id"))
  }

  @Test
  def test_EntityStoreApplicationRepository_ApplicationExists_ApplicationsRegistered_ReturnsTrue(): Unit = {
    val store = this.createStore()

    val app = new ApplicationRegistration("id", "name")
    store.registerApplication(app)

    assertTrue(store.applicationExists(app.applicationId))
  }

  @Test(expected = classOf[ApplicationNotFoundException])
  def test_EntityStoreApplicationRepository_GetApplicationRegistration_WithMissingApplicationId_ThrowsApplicationNotFoundException(): Unit = {
    val store = this.createStore()
    store.getApplicationRegistration("missingAppId")
  }

  @Test(expected = classOf[ApplicationExistsException])
  def test_EntityStoreApplicationRepository_RegisterApplication_WhenApplicationAlreadyRegistered_ThrowsApplicationExistsException(): Unit = {
    val store = this.createStore()

    val app = new ApplicationRegistration("id", "name")
    store.registerApplication(app)
    store.registerApplication(app)
  }

  @Test(expected = classOf[ApplicationNotFoundException])
  def test_EntityStoreApplicationRepository_RegisterVersion_WithMissingApplicationId_ThrowsApplicationNotFoundException(): Unit = {
    val store = this.createStore()

    val version = new ApplicationVersionRegistration("versionId", "appId", SemanticVersion(1, 0, 0))
    store.registerVersion(version, new Application(new StreamGraph()))
  }

  @Test(expected = classOf[ApplicationNotFoundException])
  def test_EntityStoreApplicationRepository_VersionExists_WithoutApplicationRegistered_ThrowsApplicationNotFoundException(): Unit = {
    val store = this.createStore()
    store.versionExists("notRegistered", SemanticVersion(1, 0, 0))
  }

  @Test
  def test_EntityStoreApplicationRepository_VersionExists_WithMatchingApplicationVersionRegistered_ReturnsTrue(): Unit = {
    val store = this.createStore()

    val version = new ApplicationVersionRegistration("versionId", "appId", SemanticVersion(1, 0, 0))
    store.registerApplication(new ApplicationRegistration("appId", "name"))
    store.registerVersion(version, new Application(new StreamGraph()))

    assertTrue(store.versionExists("appId", SemanticVersion(1, 0, 0)))
  }

  @Test
  def test_EntityStoreApplicationRepository_VersionExists_WithApplicationRegisteredButNotThatVersion_ReturnsFalse(): Unit = {
    val store = this.createStore()

    val version = new ApplicationVersionRegistration("versionId", "appId", SemanticVersion(1, 0, 0))
    store.registerApplication(new ApplicationRegistration("appId", "name"))
    store.registerVersion(version, new Application(new StreamGraph()))

    assertFalse(store.versionExists("appId", SemanticVersion(2, 0, 0)))
  }

  @Test
  def test_EntityStoreApplicationRepository_GetApplicationDefinition_WithRegisteredVersion_ReturnsDefinitionEqualToWhatWasRegistered(): Unit = {
    val store = this.createStore()

    store.registerApplication(new ApplicationRegistration("appId", "name"))

    val graph = new StreamGraph(Stream.of[IntRecord])
    val originalAppDef = new Application(graph)

    val version = new ApplicationVersionRegistration("versionId", "appId", SemanticVersion(1, 0, 0))
    store.registerVersion(version, originalAppDef)

    val actualAppDef = store.getApplicationDefinition("appId", SemanticVersion(1, 0, 0))
    assertEquals(originalAppDef, actualAppDef)
  }

  @Test(expected = classOf[ApplicationNotFoundException])
  def test_EntityStoreApplicationRepository_GetApplicationDefinition_WithUnregisteredApplication_ThrowsApplicationNotFoundException(): Unit = {
    val store = this.createStore()

    store.getApplicationDefinition("notRegistered", SemanticVersion(1, 0, 0))
  }

  @Test(expected = classOf[ApplicationVersionNotFoundException])
  def test_EntityStoreApplicationRepository_GetApplicationDefinition_WithRegisteredApplicationButUnregisteredVersion_ThrowsApplicationVersionNotFoundException(): Unit = {
    val store = this.createStore()

    store.registerApplication(new ApplicationRegistration("appId", "name"))
    store.getApplicationDefinition("appId", SemanticVersion(1, 0, 0))
  }

  @Test(expected = classOf[ApplicationNotFoundException])
  def test_EntityStoreApplicationRepository_RegisterInstanceDefinition_WithUnregisteredApplication_ThrowsApplicationNotFoundException(): Unit = {
    val store = this.createStore()

    val instance = new ApplicationInstanceDefinitionRegistration("instanceDefId", "appId", "versionId")
    store.registerInstanceDefinition(instance, new ApplicationConfiguration())
  }

  @Test(expected = classOf[ApplicationVersionNotFoundException])
  def test_EntityStoreApplicationRepository_RegisterInstanceDefinition_WithRegisteredApplicationButUnregisteredVersion_ThrowsApplicationVersionNotFoundException(): Unit = {
    val store = this.createStore()

    store.registerApplication(new ApplicationRegistration("appId", "name"))

    val instance = new ApplicationInstanceDefinitionRegistration("instanceDefId", "appId", "versionId")
    store.registerInstanceDefinition(instance, new ApplicationConfiguration())
  }

  @Test
  def test_EntityStoreApplicationRepository_GetApplicationConfiguration_ForRegisteredInstanceDefinition_ReturnsAppConfigurationEqualToWhatWasRegistered(): Unit = {
    val store = this.createStore()

    store.registerApplication(new ApplicationRegistration("appId", "name"))

    val inputStream = Stream.of[IntRecord]
    val graph = new StreamGraph(inputStream)
    val appDef = new Application(graph)

    val version = new ApplicationVersionRegistration("versionId", "appId", SemanticVersion(1, 0, 0))
    store.registerVersion(version, appDef)

    val originalConfig = new ApplicationConfiguration()
    val source = new ListDataSource[IntRecord](List())
    originalConfig.setSource(inputStream, source)

    val instance = new ApplicationInstanceDefinitionRegistration("instanceDefId", "appId", "versionId")
    store.registerInstanceDefinition(instance, originalConfig)

    val actualConfig = store.getApplicationConfiguration("appId", "instanceDefId")

    assertEquals(originalConfig, actualConfig)
  }

  @Test(expected = classOf[NoApplicationVersionsException])
  def test_EntityStoreApplicationRepository_GetLatestVersion_WithApplicationWithNoVersions_ThrowsNoApplicationVersionsException(): Unit = {
    val store = this.createStore()

    store.registerApplication(new ApplicationRegistration("appId", "name"))
    store.getLatestVersion("appId")
  }

  @Test
  def test_EntityStoreApplicationRepository_GetLatestVersion_WithApplicationWithOneVersion_ReturnsTheVersion(): Unit = {
    val store = this.createStore()

    store.registerApplication(new ApplicationRegistration("appId", "name"))
    store.registerVersion(new ApplicationVersionRegistration("version1", "appId", SemanticVersion(1, 0, 0)),
      new Application(new StreamGraph()))

    val latest = store.getLatestVersion("appId")
    assertEquals("version1", latest.applicationVersionId)
  }

  @Test
  def test_EntityStoreApplicationRepository_GetLatestVersion_WithMultipleVersions_ReturnsTheHighestVersion(): Unit = {
    val store = this.createStore()

    store.registerApplication(new ApplicationRegistration("appId", "name"))

    store.registerVersion(new ApplicationVersionRegistration("version2", "appId", SemanticVersion(2, 0, 0)),
      new Application(new StreamGraph()))

    store.registerVersion(new ApplicationVersionRegistration("version3", "appId", SemanticVersion(3, 0, 0)),
      new Application(new StreamGraph()))

    store.registerVersion(new ApplicationVersionRegistration("version1", "appId", SemanticVersion(1, 0, 0)),
      new Application(new StreamGraph()))

    val latest = store.getLatestVersion("appId")
    assertEquals("version3", latest.applicationVersionId)
  }

  @Test(expected = classOf[ApplicationNotFoundException])
  def test_EntityStoreApplicationRepository_RegisterPackage_WithUnregisteredApplication_ThrowsApplicationNotFoundException(): Unit = {
    val store = this.createStore()
    store.registerPackage(new ApplicationPackageRegistration("packageId", "appId", "instanceDefId"))
  }

  @Test(expected = classOf[ApplicationInstanceDefinitionNotFoundException])
  def test_EntityStoreApplicationRepository_RegisterPackage_WithUnregisteredInstanceDefinition_ThrowsApplicationInstanceDefinitionNotFoundException(): Unit = {
    val store = this.createStore()
    store.registerApplication(new ApplicationRegistration("appId", "name"))
    store.registerPackage(new ApplicationPackageRegistration("packageId", "appId", "instanceDefId"))
  }

  @Test(expected = classOf[ApplicationPackageNotFoundException])
  def test_EntityStoreApplicationRepository_GetPackageRegistration_WithMissingPackageId_ThrowsApplicationPackageNotFoundException(): Unit = {
    val store = this.createStore()
    store.getPackageRegistration("missingPackageId")
  }

  @Test(expected = classOf[ApplicationInstanceNotFoundException])
  def test_EntityStoreApplicationRepository_GetApplicationInstanceRegistration_WithMissingInstanceId_ThrowsApplicationInstanceNotFoundException(): Unit = {
    val store = this.createStore()
    store.getInstanceRegistration("missingInstanceId")
  }

  @Test(expected = classOf[ApplicationVersionNotFoundException])
  def test_EntityStoreApplicationRepository_GetVersionRegistration_WithMissingVersionId_ThrowsApplicationVersionNotFoundException(): Unit = {
    val store = this.createStore()
    store.getVersionRegistration("missingVersionId")
  }

  private def createStore(): EntityStoreApplicationRepository = {
    val entityStoreFactory = new MemoryEntityStoreFactory()
    new EntityStoreApplicationRepository(entityStoreFactory)
  }
}
