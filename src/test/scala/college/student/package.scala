package college

package object student {
  type Repository = delta.ddd.Repository[Student.Id, Student]
}