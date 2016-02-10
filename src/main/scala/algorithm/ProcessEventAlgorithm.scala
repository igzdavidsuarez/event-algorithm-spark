package algorithm

import societyDAO._

/**
 * Created by davidsuarez on 10/02/16.
 */
object ProcessEventAlgorithm {

  def main(args: Array[String]) {

    // TODO Load society to Test
    val society = loadSociety()

    //TODO Process Events following the previous example

  }

  def loadSociety(): Society = {
    val actors = List(Actor("id1", 0, 2))
    val conditions = List(Condition(0, "Tener saldo", null, 0, ">"))
    val consecuences = List(Consecuence(0, "Restar Saldo", null, 0, "-"))
    val events = List(Event(0, "Pago", 0, "", List(0), List(0)))
    val properties = List(Property(0, "Saldo", "Double", 0, 100))
    Society(actors, conditions, consecuences, events, properties)
  }
}

