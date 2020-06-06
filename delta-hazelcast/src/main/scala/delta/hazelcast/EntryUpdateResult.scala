package delta.hazelcast

import delta.process.Update

sealed abstract class EntryUpdateResult
final case object IgnoredDuplicate extends EntryUpdateResult
final case class MissingRevisions(range: Range) extends EntryUpdateResult
final case class Updated[S](update: Update[S]) extends EntryUpdateResult
