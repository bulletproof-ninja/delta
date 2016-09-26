package sampler.aggr

import ulysses.ddd.AggregateRoot
import scala.reflect.ClassTag

object Aggregate extends Enumeration {
  type Root = Value with AggregateRoot

  lazy val aggrs: Seq[Root] = values.toSeq.map(_.asInstanceOf[Root])
  def fromName(name: String) = withName(name).asInstanceOf[Root]

  val Employee = new Val with AggregateRoot /*[Employee, emp.EmpEvent, emp.State, Root]*/ {
    type Id = EmpId
    type Channel = Root
    def channel = this
    type Entity = Employee
    type Event = emp.EmpEvent
    type State = emp.State
    def newMutator(state: Option[State]) = new emp.Mutator(state)
    def getMutator(employee: Employee) = employee.mutator
    def init(state: State, mergeEvents: Vector[Event]) = new Employee(new emp.Mutator(state), mergeEvents)
    def checkInvariants(state: State): Unit = ()
  }

  val Department = new Val with AggregateRoot /*[Department, dept.DeptEvent, dept.State, Root]*/ {
    type Id = DeptId
    type Channel = Root
    def channel = this
    import sampler.aggr.Department.{ Impl => Dept }
    type Entity = Department
    type Event = dept.DeptEvent
    type State = dept.State
    def newMutator(state: Option[State]) = new dept.Mutator(state)
    def getMutator(dept: Department) = dept match {
      case dept: Dept => dept.mutator
    }
    def init(state: State, mergeEvents: Vector[Event]) = new Dept(new dept.Mutator(state), mergeEvents)
    def checkInvariants(state: State): Unit = ()
  }
}
