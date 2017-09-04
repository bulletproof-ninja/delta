package college

package object student {
  type Repository = delta.ddd.Repository[StudentId, Student]
}