package delta.util

import java.io.PrintStream

/** Logging agnostic interface. */
trait Logging {
  def trace(message: => String, cause: Throwable = null): Unit
  def debug(message: => String, cause: Throwable = null): Unit
  def info(message: => String, cause: Throwable = null): Unit
  def warning(message: => String, cause: Throwable = null): Unit
  def error(message: => String, cause: Throwable = null): Unit
}

object Logging {
  final val NoOp: Logging = new Logging {
    def trace(message: => String, cause: Throwable): Unit = ()
    def debug(message: => String, cause: Throwable): Unit = ()
    def info(message: => String, cause: Throwable): Unit = ()
    def warning(message: => String, cause: Throwable): Unit = ()
    def error(message: => String, cause: Throwable): Unit = ()
  }

  final val Console: Logging = new PrintStreamLogging(
    System.out, System.out, System.out, System.out, System.err)

  class PrintStreamLogging(trace: PrintStream, debug: PrintStream, info: PrintStream, warning: PrintStream, error: PrintStream)
  extends Logging {
    def trace(message: => String, cause: Throwable): Unit = trace.println(message)
    def debug(message: => String, cause: Throwable): Unit = debug.println(message)
    def info(message: => String, cause: Throwable): Unit = info.println(message)
    def warning(message: => String, cause: Throwable): Unit = warning.println(message)
    def error(message: => String, cause: Throwable): Unit = error.println(message)
  }
}