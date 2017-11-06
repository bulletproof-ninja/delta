package sampler

import delta.jdbc.ScalaEnumColumn

package object jdbc {

  implicit object AggrRootColumn extends ScalaEnumColumn[sampler.Aggr.Value](sampler.Aggr)

}
