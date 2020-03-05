package delta

import com.github.ghik.silencer.silent

package object hazelcast {

  implicit def toFirst[A](tuple: (A, _)): A = tuple._1

  @silent("never used")
  implicit def toSecond[B](a: Any, b: B): B = b

}
