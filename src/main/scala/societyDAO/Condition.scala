package societyDAO

/**
 * Created by davidsuarez on 10/02/16.
 */
case class Condition(id: Int, description: String, fkIdPreviousEvent: Int, fkIdPropertyConditioning: Int, operator:String) {}
