package airline.write.customer

import delta.write.MutableEntity

trait Repository
extends delta.write.Repository[Customer.Id, Customer]
with MutableEntity {

}
