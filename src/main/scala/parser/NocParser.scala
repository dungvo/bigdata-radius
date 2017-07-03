package parser

/**
  * Created by hungdv on 20/06/2017.
  */
class NocParser extends AbtractLogParser{
  private val text =  "(.*)"

  val time = "(\\d{2}:\\d{2}:\\d{2})"
  //  val date = "(\\w{3,}  \\d{1,})"
  val date = "(\\w{3,}\\s{1,}\\d{1,})"

  val header = "<([0-9]+)>"

  val deviceName = "([A-Z][A-Z0-9.-]+)"

  val foundResult = "([A-Z0-9-_]+)"

}
