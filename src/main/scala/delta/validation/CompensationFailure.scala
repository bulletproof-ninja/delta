package delta.validation

/**
  * Compensating action failed.
  *
  * @param stream The stream identifier
  * @param cause The cause of failure
  */
class CompensationFailure(stream: Any, cause: Throwable)
extends Exception(s"Stream $stream failed compensating action", cause)
