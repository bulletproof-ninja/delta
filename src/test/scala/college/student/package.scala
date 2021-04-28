package college

import scuff.Codec

package object student {

  def Entity = Student
  def Channel = Student.channel

  implicit def StudentId(int: Int): Student.Id = new Student.Id(int)

  implicit val StudentIdCodec = new Codec[Int, Student.Id] {
    def encode(a: Int): IntId[Student] = new Student.Id(a)
    def decode(b: IntId[Student]): Int = b.int
  }

}
