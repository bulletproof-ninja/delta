package college

package object student {

  implicit def StudentId(int: Int): Student.Id = new Student.Id(int)

}
