package Combined

object argument {
  def isArgsValid(args: Array[String]): Boolean = {
    if (args.length < 4) false
    else true
  }
}
