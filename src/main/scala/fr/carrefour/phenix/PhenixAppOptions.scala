package fr.carrefour.phenix

class PhenixAppOptions extends ScalaApp {
  val inputFolderOption: Symbol = 'input_folder
  val regexFolder = "data\\/examples|data\\/generated"

  def parseCmdLineArgs(args: Array[String]): OptionMap = {
    parseCmdLine(args, List(inputFolderOption))
  }

  override def checkOptions(param_options: OptionMap, stdin_size_diff: Int, invalid_options: List[String], app_name: String): Int = {
    var status = 0
    status += super.checkOptions(param_options, stdin_size_diff, invalid_options, app_name)
    status += checkOption(inputFolderOption, param_options, (logsName: String) => logsName.matches(regexFolder), isMandatory = true, app_name)
    if(status > 0) {
      printUsage(app_name)
    }
    status
  }


  def printUsage(appName: String): Unit = {
    println("Usage: " + appName +
      s""" [OPTIONS]
         | Mandatory option :
         | --input_folder    name of input folder
            """.
        stripMargin)
  }
}
