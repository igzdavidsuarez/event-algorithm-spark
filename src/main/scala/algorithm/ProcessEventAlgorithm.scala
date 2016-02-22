package algorithm

import org.apache.spark.{SparkContext, SparkConf}
import societyDAO._

/**
 * Created by davidsuarez on 10/02/16.
 */
object ProcessEventAlgorithm {

  def main(args: Array[String]) {

    // Init spark Context
    val conf = new SparkConf().setMaster("local[2]").setAppName("ProcessAlgorithm")
    val sc = new SparkContext(conf)


    // Simulated event list to process
    val eventListToProcess = List(ActorEvent("id1", 0, 10))
    // TODO Load society from DB instead of Hardcode
    val society = loadSociety()

    // Process event list from the initial society state.
    processEvents(eventListToProcess, society).foreach(println)

    // Stop Spark Context
    sc.stop()

  }

  /**
   * Load Society from DB
   *
   * @return
   */
  def loadSociety(): Society = {

    // TODO Load form DB instead of Hardcode
    val actorsProperties = List(ActorProperty("id1", 0, 2))
    val conditions = List(Condition(0, "Tener saldo",/*This value should be null*/ 0, 0, ">"))
    val consecuences = List(Consecuence(0, "Restar Saldo",/*This value should be null*/ 0, 0, "-"))
    val events = List(Event(0, "Pago", 0, "IDK", List(0), List(0)))
    val properties = List(Property(0, "Saldo", "Double", 0, 100))

    Society(actorsProperties, conditions, consecuences, events, properties)
  }


  /**
   * Process all events and send to the core algorithm. Finally return the final event list
   *
   * @param eventList
   * @param society
   * @return
   */
  def processEvents(eventList: List[ActorEvent], society: Society): List[ActorEvent] = {

    val accum = (society, List(ActorEvent("id1", 0, 10)))

    val outputEventList = eventList.foldLeft(accum)((a, b) => (processEvent(a, b)))

    outputEventList._2
  }

  /**
   * Process an event core algorithm
   *
   * @param acc - Tuple made by a society state and an event to process
   * @param eventValue
   * @return
   */
  def processEvent(acc: (Society, List[ActorEvent]), eventValue: ActorEvent): (Society, List[ActorEvent]) = {
    var society = acc._1
    var eventList = acc._2

    var conditions = society.conditions
    var consecuences = society.consecuences
    var properties = society.properties

    // Check Conditions
    if (checkConditions(conditions, properties)) {

      // Check Consecuences return a tuple with the new society and all the events triggered
      val societyAndEventList = checkConsecuences(society, eventValue)

      // TODO add to the event list the event processed instead of concatenate lists.
      eventList = eventList ::: societyAndEventList._2
      (societyAndEventList._1, eventList)
    } else {
      (society, eventList)
    }
  }


  /**
   * Check all conditions
   *
   * @param conditions
   * @param properties
   * @return Boolean
   */
  def checkConditions(conditions: List[Condition], properties: List[Property]): Boolean = {
    // TODO
    true
  }


  /**
   * Check all consecuences to change the society state and run all the events triggered
   *
   * @param society
   * @param actorEvent
   * @return (Society, List[ActorEvent])
   */
  def checkConsecuences(society: Society, actorEvent: ActorEvent): (Society, List[ActorEvent]) = {

    val event = society.events.find(_.id == actorEvent.fkIdEvent).get
    val consecuenceIds = event.aFkIdConsecuences

    // Iterate to run consecuence one by one
    val newSociety = consecuenceIds.foldLeft((society, List(actorEvent)))((a, b) => (executeConsecuence(a, b)))

    // Return new Society
    newSociety
  }


  /**
   * Execute consecuence and return the new society state
   *
   * @param acc
   * @param consecuenceId
   * @return Society
   */
  def executeConsecuence(acc: (Society, List[ActorEvent]), consecuenceId: Int): (Society, List[ActorEvent]) = {

    val society = acc._1
    val listEvent = acc._2
    val actorEvent =  listEvent.last // You have to take the last one
    val event = society.events.find(_.id == actorEvent.fkIdEvent).get

    // Find consecuence in society
    val consecuence = society.consecuences.find(_.id == consecuenceId).get

    val eventTriggeredId = consecuence.fkIdEventTriggered
    val propertyAlteredId = consecuence.fkIdPropertyAltered

    if (eventTriggeredId != 0) {
     //TODO CALL TO THE RECURSIVE ALGORITHM AND RETURN THE NEW SOCIETY AND THE LIST PROCESSED

      // Run the event triggered calling to the algorithm recursively and add the result to the return list
      val eventTriggered = society.events.find(_.id == eventTriggeredId).get

      //TODO CALL THE PROCESS EVENT ALGORITHM AND CONCAT THE RESULT TO THE LIST

      //TODO CREATE THE NEW SOCIETY RETURNED FROM THE PROCESS EVENT ALGORITHM AND RETURN THE NEW ONE

      //Return the new (Society, List of events)
      (society, List(ActorEvent("id1", 0, 1)))
    } else {

      val actorProperty = society.actorsProperties.find(_.id == actorEvent.id).get
      val operator = consecuence.operator
      var newPropertyValue = actorProperty.value
      operator match {
        case "+" => actorProperty.value = actorProperty.value + actorEvent.value
        case "-" => actorProperty.value = actorProperty.value - actorEvent.value
        case "*" => actorProperty.value = actorProperty.value * actorEvent.value
        case "/" => actorProperty.value = actorProperty.value / actorEvent.value
      }

      (society, listEvent)
    }
  }
}

