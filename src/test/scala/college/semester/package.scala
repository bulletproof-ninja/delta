package college

package object semester {
  type Repository = delta.ddd.Repository[SemesterId, Semester]
}
