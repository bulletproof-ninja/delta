package sampler

import scuff.serialVersionUID
import ulysses.EventCodec
import ulysses.util.ReflectiveDecoder
import language.implicitConversions
import ulysses.jdbc.ScalaEnumColumn
import java.sql.ResultSet
import sampler.aggr.dept.DeptEvent
import sampler.aggr.emp.EmpEvent

package object jdbc {

  implicit object AggrRootColumn extends ScalaEnumColumn[sampler.Aggr.Value](sampler.Aggr)

}
