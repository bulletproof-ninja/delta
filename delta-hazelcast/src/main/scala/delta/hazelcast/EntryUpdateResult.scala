package delta.hazelcast

import delta.process.Update

sealed abstract class EntryUpdateResult
case object IgnoredDuplicate extends EntryUpdateResult
case class MissingRevisions(range: Range) extends EntryUpdateResult
case class Updated[S](update: Update[S]) extends EntryUpdateResult
