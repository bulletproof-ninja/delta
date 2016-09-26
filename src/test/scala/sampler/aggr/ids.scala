package sampler.aggr

import java.util.UUID

sealed abstract class Id[T] {
  def uuid: UUID
}

case class DeptId(uuid: UUID = UUID.randomUUID) extends Id[Department]
case class EmpId(uuid: UUID = UUID.randomUUID) extends Id[Employee]
