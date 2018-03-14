package delta.java

import delta.NoVersioning

abstract class EventCodec[EVT, SF] extends delta.EventCodec[EVT, SF]

abstract class NoVersionEventCodec[EVT, SF]
  extends EventCodec[EVT, SF]
  with NoVersioning[EVT, SF]
