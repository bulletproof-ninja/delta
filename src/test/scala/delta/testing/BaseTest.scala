package delta.testing

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest._
import java.util.concurrent.CopyOnWriteArrayList
import scala.jdk.CollectionConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import java.io.{PrintWriter, File, PrintStream}

abstract class BaseTest
extends AnyFunSuite
with BeforeAndAfterEach
with BeforeAndAfterAll {

  protected val instance: Long = System.currentTimeMillis/1000

  protected lazy val logFile = {
    val file = new File(s"../${getClass.getName}-$instance.log")
    file.createNewFile()
    file
  }

  protected lazy val printStream: PrintStream = {
    val streamFile = new File(s"../${getClass.getName}-stream-$instance.log")
    streamFile.createNewFile()
    new PrintStream(streamFile, "UTF-8")
  }
  protected lazy val printWriter: PrintWriter = {
    val writerFile = new File(s"../${getClass.getName}-writer-$instance.log")
    writerFile.createNewFile()
    new PrintWriter(writerFile, "UTF-8")
  }

  protected implicit def toFuture[T](t: T): Future[T] = Future successful t

  override protected def beforeEach() = {
    _failures.clear()
  }
  override protected def afterEach(): Unit = {
    _failures.forEach(_.printStackTrace(System.err))
    assert(_failures.isEmpty)
  }

  private[this] val _failures = new CopyOnWriteArrayList[Throwable]
  protected def failures = _failures.asScala.toList
  implicit protected val ec: ExecutionContext = new RandomDelayExecutionContext {
    override def reportFailure(th: Throwable): Unit = {
      super.reportFailure(th)
      _failures add th
    }
  }


}
