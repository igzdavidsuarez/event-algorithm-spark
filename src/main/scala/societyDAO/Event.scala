package societyDAO

/**
 * Created by davidsuarez on 10/02/16.
 */
case class Event(id: Int, description: String, fkIdPropertyRangeOfValue: Int, value: String, aFkIdConditions: List[Int], aFkIdConsecuences: List[Int]) {}
