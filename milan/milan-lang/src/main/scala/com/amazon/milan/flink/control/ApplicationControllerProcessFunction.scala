package com.amazon.milan.flink.control

import java.io.{PrintWriter, StringWriter}
import java.time.Duration

import com.amazon.milan.control.{ApplicationControllerMessageEnvelope, ApplicationControllerState}
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimerService
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory


/**
 * A Flink [[ProcessFunction]] that wraps an [[ApplicationControllerMessageHandler]] and processes application controller messages
 * that arrive on a stream.
 *
 * @param applicationControllerMsgHandler The [[ApplicationControllerMessageHandler]] to send messages to.
 * @param applicationStateCheckPeriod     The period between checking for changes to running Flink applications.
 * @param snapshotBaseDirectory           Base directory where the regular snapshots should be stored.
 */
class ApplicationControllerProcessFunction(applicationControllerMsgHandler: ApplicationControllerMessageHandler,
                                           applicationStateCheckPeriod: Duration,
                                           diagnosticOutputTag: OutputTag[ApplicationControllerDiagnostic],
                                           snapshotBaseDirectory: String,
                                           snapshotIntervalInMinutes: Int)
  extends KeyedProcessFunction[Tuple, ApplicationControllerMessageEnvelope, ApplicationControllerState]
    with Serializable {

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))
  @transient private var controllerState: ValueState[ApplicationControllerState] = _
  @transient private var hasTimer: Boolean = false
  @transient private var alreadyLoadedState: Boolean = false

  private val snapshotIntervalDuration: Duration = Duration.ofMinutes(snapshotIntervalInMinutes)
  private var lastSnapshotTimestamp: ValueState[Long] = _

  /**
   * Process an incoming message.
   *
   * @param message   The message envelope.
   * @param context   Message context.
   * @param collector Collector for the state output stream.
   */
  override def processElement(message: ApplicationControllerMessageEnvelope,
                              context: KeyedProcessFunction[Tuple, ApplicationControllerMessageEnvelope, ApplicationControllerState]#Context,
                              collector: Collector[ApplicationControllerState]): Unit = {
    val diag = new DiagnosticWriter(this.logger, context, this.diagnosticOutputTag)

    // The stream is keyed by controllerId.
    val controllerId = context.getCurrentKey.getField[String](0)
    diag.debug(s"Processing message to controller '$controllerId'.")

    if (this.controllerState.value() != null && !this.alreadyLoadedState) {
      diag.info(s"Loading controller state for controller '$controllerId'.")

      try {
        this.applicationControllerMsgHandler.loadState(this.controllerState.value())
      }
      catch {
        case ex: Exception =>
          diag.error("Error loading controller state.", ex)
      }

      this.alreadyLoadedState = true
    }

    try {
      val result = this.applicationControllerMsgHandler.processMessage(message.messageBodyJson)

      if (result.success) {
        diag.info(result.details)
      }
      else {
        diag.error(result.details)
      }
    }
    catch {
      case ex: Exception =>
        diag.error("Error processing message.", ex)
    }

    this.updateAndOutputState(controllerId, collector)

    // The first time we get a message we need to start the timer to periodically check for changes to the list of
    // running Flink applications.
    if (!this.hasTimer) {
      this.startStateCheckTimer(diag, context.timerService())
    }
  }

  /**
   * Check for and handle application controller state changes.
   *
   * @param timestamp The current time stamp.
   * @param context   Timer context.
   * @param collector Collector for the state output stream.
   */
  override def onTimer(timestamp: Long,
                       context: KeyedProcessFunction[Tuple, ApplicationControllerMessageEnvelope, ApplicationControllerState]#OnTimerContext,
                       collector: Collector[ApplicationControllerState]): Unit = {
    val diag = new DiagnosticWriter(this.logger, context, this.diagnosticOutputTag)

    val controllerId = context.getCurrentKey.getField[String](0)

    diag.debug(s"Timer elapsed for controller '$controllerId'.")

    // Check for any changes to the running Flink applications.
    // If there are any changes then update the state.
    try {
      if (this.applicationControllerMsgHandler.checkForChanges()) {
        this.updateAndOutputState(controllerId, collector)
      }
    }
    catch {
      case ex: Exception =>
        diag.error("Error checking for Flink application changes.", ex)
    }

    // Check and trigger snapshot if required
    try {
      if (this.checkIfSnapshotIsRequired(diag, timestamp)) {
        diag.debug("Triggering snapshot")
        this.applicationControllerMsgHandler.snapshotApplications(this.snapshotBaseDirectory)
        this.lastSnapshotTimestamp.update(timestamp)
      }
      else {
        diag.debug(s"Snapshot not required at timestamp: $timestamp")
      }
    }
    catch {
      case ex: Exception =>
        diag.error("Error triggering snapshot.", ex)
    }

    this.startStateCheckTimer(diag, context.timerService())
  }

  /**
   * Called by Flink when a new instance of the class is created.
   *
   * @param parameters Application configuration parameters.
   */
  override def open(parameters: Configuration): Unit = {
    val controllerStateDescriptor = new ValueStateDescriptor[ApplicationControllerState]("controllerState", TypeInformation.of(classOf[ApplicationControllerState]))
    this.controllerState = this.getRuntimeContext.getState(controllerStateDescriptor)
    val lastSnapshotTimestampDescriptor = new ValueStateDescriptor[Long]("lastSnapshotTimestamp", TypeInformation.of(classOf[Long]))
    this.lastSnapshotTimestamp = this.getRuntimeContext.getState(lastSnapshotTimestampDescriptor)
  }

  /**
   * Update the controller state in the Flink state fields, and output the state to a stream.
   *
   * @param controllerId The ID of this controller.
   * @param collector    A [[Collector]] for the state output stream.
   */
  private def updateAndOutputState(controllerId: String, collector: Collector[ApplicationControllerState]): Unit = {
    this.logger.info(s"Updating application controller state.")

    // Get the current state of the controller and save it so that it will be restored if the node goes down.
    val state = this.applicationControllerMsgHandler.saveState(controllerId)

    this.controllerState.update(state)

    // Write the state to the output stream so that it's visible externally.
    this.logger.info(s"Sending application controller state to output stream.")
    collector.collect(state)
  }

  /** Check if it's time to trigger a snapshot */
  private def checkIfSnapshotIsRequired(diag: DiagnosticWriter, currentTimestamp: Long): Boolean = {
    var isSnapshotRequired: Boolean = false
    if (this.lastSnapshotTimestamp.value() == 0) {
      // Trigger a snapshot if none has been triggered yet
      diag.info("No snapshot has been triggered yet")
      isSnapshotRequired = true
    }
    else { // Trigger a snapshot if enough time has passed since the last one
      val nextSnapshotTimestamp = this.lastSnapshotTimestamp.value() + this.snapshotIntervalDuration.toMillis
      if (currentTimestamp >= nextSnapshotTimestamp) {
        diag.info(s"Enough time has passed since last snapshot timestamp: ${this.lastSnapshotTimestamp.value()}. Current timestamp: $currentTimestamp")
        isSnapshotRequired = true
      }
    }
    isSnapshotRequired
  }

  /**
   * Start the timer that will trigger the next state check.
   *
   * @param diag         A writer for diagnostic messages.
   * @param timerService The timer service used to register the timer.
   */
  private def startStateCheckTimer(diag: DiagnosticWriter, timerService: TimerService): Unit = {
    this.hasTimer = true

    val currentTimeMs = timerService.currentProcessingTime()
    val timerTime = currentTimeMs + this.applicationStateCheckPeriod.toMillis

    diag.debug(s"Starting timer for time stamp $timerTime.")
    timerService.registerProcessingTimeTimer(timerTime)
  }

  private class DiagnosticWriter(logger: Logger,
                                 context: KeyedProcessFunction[Tuple, ApplicationControllerMessageEnvelope, ApplicationControllerState]#Context,
                                 outputTag: OutputTag[ApplicationControllerDiagnostic]) {
    def debug(message: String): Unit = {
      this.logger.debug(message)
      this.write(new ApplicationControllerDiagnostic(ApplicationControllerDiagnostic.DEBUG, message))
    }

    def info(message: String): Unit = {
      this.logger.info(message)
      this.write(new ApplicationControllerDiagnostic(ApplicationControllerDiagnostic.INFO, message))
    }

    def warning(message: String): Unit = {
      this.logger.warn(message)
      this.write(new ApplicationControllerDiagnostic(ApplicationControllerDiagnostic.WARNING, message))
    }

    def error(message: String, ex: Exception): Unit = {
      this.logger.error(message, ex)

      val stackTraceStringWriter = new StringWriter()
      val stackTracePrintWriter = new PrintWriter(stackTraceStringWriter)
      ex.printStackTrace(stackTracePrintWriter)
      val fullMessage = s"$message Error details: ${ex.getMessage}\n${stackTraceStringWriter.toString}"
      this.write(new ApplicationControllerDiagnostic(ApplicationControllerDiagnostic.ERROR, fullMessage))
    }

    def error(message: String): Unit = {
      this.logger.error(message)
      this.write(new ApplicationControllerDiagnostic(ApplicationControllerDiagnostic.ERROR, message))
    }

    def write(message: ApplicationControllerDiagnostic): Unit = {
      this.context.output(this.outputTag, message)
    }
  }

}
