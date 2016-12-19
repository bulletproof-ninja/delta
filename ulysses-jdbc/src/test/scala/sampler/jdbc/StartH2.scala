package sampler

object StartH2 {
  def main(args: Array[String]) {
    val server = new org.h2.server.TcpServer
    server.start()
    println(s"Server started: ${server.getURL}")
    while (!Thread.currentThread.isInterrupted) {
      Thread sleep Long.MaxValue
    }
  }
}
