package college

package object student {
  type Repository = scuff.ddd.Repository[StudentId, Student]
}