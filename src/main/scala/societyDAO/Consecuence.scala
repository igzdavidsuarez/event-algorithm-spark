package societyDAO

/**
 * Created by davidsuarez on 10/02/16.
 */
case class Consecuence(id: Int, description: String, fkIdEventTriggered:Int, fkIdPropertyAltered: Int, operator: String) {}
