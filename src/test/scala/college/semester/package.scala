package college

package object semester {
  type Repository = delta.ddd.Repository[Semester.Id, Semester]
}
