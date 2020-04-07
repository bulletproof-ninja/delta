package college

package object student {
  type Repository = delta.write.Repository[Student.Id, Student]
}
