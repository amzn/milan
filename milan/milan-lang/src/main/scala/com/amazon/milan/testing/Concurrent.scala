package com.amazon.milan.testing

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}


object Concurrent {
  /**
   * Executes an action synchronously and then waits until a predicate is true, or a timeout occurs.
   *
   * @param action    The action to execute.
   * @param predicate The predicate that indicates the operation is complete.
   * @param timeout   The maximum time to wait for the predicate to return true.
   * @return A [[Future]]`[`Boolean`]` that returns the final value returned from the predicate.
   *         False indicates the the operation timed out.
   */
  def executeAsync(action: () => Any, predicate: () => Boolean, timeout: Duration): Future[Boolean] = {
    Future {
      executeAndWait(action, predicate, timeout)
    }(ExecutionContext.global)
  }

  /**
   * Executes an action synchronously and then waits until a predicate is true, or a timeout occurs.
   *
   * @param action    The action to execute.
   * @param predicate The predicate that indicates the operation is complete.
   * @param timeout   The maximum time to wait for the predicate to return true.
   * @return The final value returned from the predicate. False indicates the the operation timed out.
   */
  def executeAndWait(action: () => Any, predicate: () => Boolean, timeout: Duration): Boolean = {
    action()
    this.wait(predicate, timeout)
  }

  /**
   * Waits until a predicate is true, or a timeout occurs.
   *
   * @param predicate The predicate that indicates the operation is complete.
   * @param timeout   The maximum time to wait for the predicate to return true.
   * @return The final value returned from the predicate. False indicates the the operation timed out.
   */
  def wait(predicate: () => Boolean, timeout: Duration): Boolean = {
    this.wait(predicate, () => (), timeout)
  }

  /**
   * Waits until a predicate is true, or a timeout occurs.
   *
   * @param predicate The predicate that indicates the operation is complete.
   * @param shutdown  An action to invoke after the execution completes or times out.
   * @param timeout   The maximum time to wait for the predicate to return true.
   * @return The final value returned from the predicate. False indicates the the operation timed out.
   */
  def wait(predicate: () => Boolean, shutdown: () => Any, timeout: Duration): Boolean = {
    waitInternal(predicate, timeout)

    shutdown()

    // Return the result of the predicate at the time execution finished.
    predicate()
  }

  /**
   * Executes an action asynchronously and waits until a predicate is true, or a timeout occurs.
   *
   * @param action    The action to execute.
   * @param predicate The predicate that indicates the operation is complete.
   * @param timeout   The maximum time to wait for the predicate to return true.
   * @return The final value returned from the predicate. False indicates the the operation timed out.
   * @return
   */
  def executeAsyncAndWait(action: () => Any,
                          predicate: () => Boolean,
                          timeout: Duration): Boolean = {
    this.executeAsyncAndWait(action, predicate, _ => (), timeout)
  }

  /**
   * Executes an action asynchronously and waits until a predicate is true, or a timeout occurs.
   *
   * @param action    The action to execute.
   * @param predicate The predicate that indicates the operation is complete.
   * @param shutdown  An action to invoke after the execution completes or times out.
   * @param timeout   The maximum time to wait for the predicate to return true.
   * @return The final value returned from the predicate. False indicates the the operation timed out.
   * @return
   */
  def executeAsyncAndWait(action: () => Any,
                          predicate: () => Boolean,
                          shutdown: Thread => Any,
                          timeout: Duration): Boolean = {
    val runnable = new Runnable {
      override def run(): Unit = action()
    }

    val executionThread = new Thread(runnable)
    executionThread.start()

    // Calculate the time at which we must return.
    val stopTime = Instant.now().plus(timeout)

    // Wait for the predicate to be true or the timeout.
    this.waitInternal(predicate, timeout)

    // Execute the shutdown logic.
    shutdown(executionThread)

    // How long we can wait for the thread to gracefully shutdown.
    val shutdownTimeout = Duration.between(Instant.now(), stopTime)

    if (!shutdownTimeout.isNegative) {
      this.waitInternal(() => !executionThread.isAlive, shutdownTimeout)
    }

    // Kill the thread if it's still going.
    if (executionThread.isAlive) {
      executionThread.interrupt()
    }

    // Return the result of the predicate at the time execution finished.
    predicate()
  }

  /**
   * Waits until a predicate is true, or a timeout occurs.
   *
   * @param predicate The predicate that indicates the operation is complete.
   * @param timeout   The maximum time to wait for the predicate to return true.
   */
  private def waitInternal(predicate: () => Boolean, timeout: Duration): Unit = {
    val stopTime = Instant.now().plus(timeout)

    while (Instant.now().isBefore(stopTime) && !predicate()) {
      Thread.sleep(10)
    }
  }

  /**
   * Executes an action and waits for the action to complete and for a predicate to be true.
   * Returns true if the action completes and the predicate is true afterwards, otherwise returns false.
   *
   * @param action    The action to execute.
   * @param predicate The predicate to test for.
   * @param timeout   The maximum time to wait for the action and the predicate.
   * @return True if the action completes in time and the predicate is true afterwards, otherwise False.
   */
  def executeUntil(action: () => Any,
                   predicate: () => Boolean,
                   timeout: Duration): Boolean = {
    val runnable = new Runnable {
      override def run(): Unit = action()
    }

    val executionThread = new Thread(runnable)
    executionThread.start()

    // Wait until the thread is finished and the predicate is true.
    this.waitInternal(() => !executionThread.isAlive && predicate(), timeout)

    // If the thread is still going, kill it and return false because the action was supposed to finish.
    if (executionThread.isAlive) {
      executionThread.interrupt()
      false
    }
    else {
      // Return the result of the predicate at the time execution finished.
      predicate()
    }
  }
}
