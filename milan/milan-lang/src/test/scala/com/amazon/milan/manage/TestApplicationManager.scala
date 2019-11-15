package com.amazon.milan.manage

import java.io.ByteArrayInputStream

import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.lang.StreamGraph
import com.amazon.milan.storage.MemoryEntityStoreFactory
import com.amazon.milan.{Id, SemanticVersion}
import org.junit.Assert._
import org.junit.Test


@Test
class TestApplicationManager {
  @Test
  def test_ApplicationManager_StartNewVersion_RegistersAllApplicationEntities(): Unit = {
    val entityStoreFactory = new MemoryEntityStoreFactory()
    val appRepo = new EntityStoreApplicationRepository(entityStoreFactory)

    val packageRepo = PackageRepository.createMemoryPackageRepository()
    val packager = new EmptyArrayPackager(packageRepo)

    val controllerClient = new NoOpControllerClient(packageRepo)

    val manager = new ApplicationManager(appRepo, packager, controllerClient)

    val appReg = new ApplicationRegistration("appId", "app")
    appRepo.registerApplication(appReg)

    val appDef = new Application("appId", new StreamGraph())
    val config = new ApplicationConfiguration()

    val instanceId = manager.startNewVersion(SemanticVersion(1, 0, 0), appDef, config)

    val instanceRegistration = appRepo.getInstanceRegistration(instanceId)
    assertEquals(appReg.applicationId, instanceRegistration.applicationId)

    val packageRegistration = appRepo.getPackageRegistration(instanceRegistration.packageId)
    assertEquals(appReg.applicationId, packageRegistration.applicationId)

    val instanceDefinitionRegistration = appRepo.getInstanceDefinitionRegistration(packageRegistration.instanceDefinitionId)
    assertEquals(appReg.applicationId, instanceDefinitionRegistration.applicationId)
    assertEquals(instanceRegistration.instanceDefinitionId, instanceDefinitionRegistration.instanceDefinitionId)

    val versionRegistration = appRepo.getVersionRegistration(instanceDefinitionRegistration.applicationVersionId)
    assertEquals(appReg.applicationId, versionRegistration.applicationId)
    assertEquals(SemanticVersion(1, 0, 0), versionRegistration.version)

    // All we care about for these last three are that we get the object back without any exception being thrown.
    val applicationRegistration = appRepo.getApplicationRegistration(versionRegistration.applicationId)
    assertEquals(appReg.applicationId, applicationRegistration.applicationId)

    val repoAppDef = appRepo.getApplicationDefinition(versionRegistration.applicationId, versionRegistration.version)
    assertEquals(appDef, repoAppDef)

    val repoConfig = appRepo.getApplicationConfiguration(instanceDefinitionRegistration.applicationId, instanceDefinitionRegistration.instanceDefinitionId)
    assertEquals(config, repoConfig)
  }
}


class EmptyArrayPackager(packageRepository: PackageRepository) extends ApplicationPackager {
  override def packageApplication(instanceDefinition: ApplicationInstance): String = {
    val packageId = Id.newId()
    this.packageRepository.copyToRepository(packageId, new ByteArrayInputStream(Array()))
    packageId
  }
}
