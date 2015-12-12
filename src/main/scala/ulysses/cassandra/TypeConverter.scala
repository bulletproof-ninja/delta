package ulysses.cassandra

import scala.reflect.ClassTag
import com.datastax.driver.core.Row

trait TypeConverter[T] {
  type CT <: AnyRef
  def cassandraType: ClassTag[CT]
  def getValue(row: Row, column: Int): T
  def toCassandraType(t: T): CT
}
