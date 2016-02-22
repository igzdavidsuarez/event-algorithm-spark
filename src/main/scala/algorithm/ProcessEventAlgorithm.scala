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

    // TODO Load society from DB instead of Hardcode
    // Simulated event list to process and Society status
    val eventListToProcess = List(ActorEvent("id1", 0, 10))
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
    val actorsProperties = List(ActorProperty("id1", 0, 10))
    val conditions = List(Condition(0, "Enough balance",/*This value should be null*/ 0, 0, ">"))
    val consecuences = List(Consecuence(0, "Restar Saldo",/*This value should be null*/ 0, 0, "-"), Consecuence(1, "Raise Payment",/*This value should be null*/ 1, 0, "-"), Consecuence(1, "Raise Extra Payment",/*This value should be null*/ 0, 0, "-"))
    val events = List(Event(0, "Payment", 0, "IDK", List(0), List(0, 1)), Event(1, "Get salary", 0, "IDK", List(0), List(0)))
    val properties = List(Property(0, "Balance", "Double", 0, 100))

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

    val accum = (society, List(ActorEvent("start", 0, 0)))

    val outputEventList = eventList.foldLeft(accum)((a, b) => (processEvent(a, b)))


    println(outputEventList._1.actorsProperties)
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

    // Get Consecuences Ids
    val event = society.events.find(_.id == actorEvent.fkIdEvent).get
    val consecuenceIds = event.aFkIdConsecuences

    // Iterate to run consecuences and changing society status one by one
    val newSociety = consecuenceIds.foldLeft((society, List(actorEvent)))((a, b) => executeConsecuence(a, b))

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

    // Find consecuence in society
    val consecuence = society.consecuences.find(_.id == consecuenceId).get

    // Check if the consecuence raise another event
    val eventTriggeredId = consecuence.fkIdEventTriggered

    if (eventTriggeredId != 0) {

      // Run the event triggered calling to the algorithm recursively and add the result to the return list
      val eventTriggered = society.events.find(_.id == eventTriggeredId).get

      // TODO Change '10' to value taken from DB
      // TODO run event to another actor.
      val eventsTriggered = processEvent((society, List()), ActorEvent(actorEvent.id, eventTriggeredId, 10))

      //Return the new (Society, List of events)
      (eventsTriggered._1, listEvent ::: eventsTriggered._2)

    } else {

      // The consecuence just change an actor property
      val actorProperty = society.actorsProperties.find(_.id == actorEvent.id).get
      val operator = consecuence.operator

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

