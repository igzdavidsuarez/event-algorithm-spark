package algorithm

import org.apache.spark.{SparkContext, SparkConf}
import societyDAO._

/**
 * Created by davidsuarez on 10/02/16.
 */
object ProcessEventAlgorithm {

  def main(args: Array[String]) {


    val conf = new SparkConf().setMaster("local[2]").setAppName("ProcessAlgorithm")
    val sc = new SparkContext(conf)


    // TODO Load society to Test
    val society = loadSociety()
//    society.actors.foreach(println)
//    society.conditions.foreach(println)
//    society.consecuences.foreach(println)
//    society.properties.foreach(println)
//    society.events.foreach(println)

    //TODO Process Events following the previous example

    processEvents(society.events, society).foreach(println)

    sc.stop()

  }

  def loadSociety(): Society = {
    val actors = List(Actor("id1", 0, 2))
    val conditions = List(Condition(0, "Tener saldo",/*This value should be null*/ 0, 0, ">"))
    val consecuences = List(Consecuence(0, "Restar Saldo",/*This value should be null*/ 0, 0, "-"))
    val events = List(Event(0, "Pago", 0, "IDK", List(0), List(0)))
    val properties = List(Property(0, "Saldo", "Double", 0, 100))
    Society(actors, conditions, consecuences, events, properties)
  }

  def processEvents(eventList: List[Event], society: Society): List[Event] = {

    val accum = (society, List(Event(0, "Start", 0, "Start", List(0), List(0))))

    val outputEventList = eventList.foldLeft(accum)((a, b) => (processEvent(a, b)))

    outputEventList._2
  }

  def processEvent(acc: (Society, List[Event]), eventValue: Event): (Society, List[Event]) = {
    var society = acc._1
    var eventList = acc._2

    var conditions = society.conditions
    var consecuences = society.consecuences
    var properties = society.properties

    // Check Conditions
    if (checkConditions(conditions, properties)) {

      // Check Consecuences
      society = checkConsecuences(society)

      // TODO add to the event list the event processed instead of concatenate lists.
      eventList = eventList ::: List(eventValue)
    }

    (society, eventList)
  }

  def checkConditions(conditions: List[Condition], properties: List[Property]): Boolean = {
    // TODO
    true
  }


  def checkConsecuences(society: Society): Society = {

    // Change the properties
    val newMax = society.properties(0).max + 5
    val newProperties = List(Property(0, "Saldo", "Double", 0, newMax))

    val newSociety = Society(society.actors, society.conditions, society.consecuences, society.events, newProperties)

    // Return new Society
    newSociety
  }
}

