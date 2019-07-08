package fr.carrefour.phenix


import scala.util.matching.Regex

trait ScalaApp extends App{
  type OptionListMap = Map[Symbol, List[String]]
  type OptionMap = Map[Symbol, String]

  /**
    * Add optionValue to the list options(optionName),
    * or add (optionName, optionValue) entry if options(optionName) is not defined
    *
    *  @param optionName the entry key to add to the options map
    *  @param optionValue the entry value to add to the options map
    *  @return a new options map containing the new value optionValue
    */
  def appendOption(
                    optionName: Symbol,
                    optionValue: String,
                    options: OptionListMap): OptionListMap = {
    if (!options.isDefinedAt(optionName)) {
      // add a new entry (optionName -> optionValue) if options(optionName) is not defined
      options ++ Map(optionName -> List(optionValue))
    } else {
      // add a new element optionValue to the list options(optionName)
      options - optionName ++ Map(optionName -> (optionValue :: options(optionName)))
    }
  }

  /**
    * Check if a mandatory option is missing, and/or if it is formatted correctly
    *
    *  @param optionName the option name to check
    *  @param optionValue the options map
    *  @param validateOption the function to check if the option is formatted correctly
    *  @param isMandatory true if the option is mandatory (i.e. must be defined on the command line)
    *  @param appName the spark application name
    *  @return 0 if the check is successful, 1 otherwise
    */
  def checkOption(
                   optionName: Symbol,
                   options: OptionMap,
                   validateOption: String => Boolean,
                   isMandatory: Boolean,
                   appName: String): Int = {
    if (isMandatory && !options.isDefinedAt(optionName)) {
      println(appName + ": missing mandatory option '--" + optionName.name + "'")
      return 1
    } else if (options.isDefinedAt(optionName) && !validateOption(options(optionName))) {
      println(appName + ": invalid parameter for --" +
        optionName.name + ": '" + options(optionName) + "'")
      return 1
    }
    return 0
  }

  /**
    * Check the command line options map
    *
    *  @param paramOptions the command line parameter options map
    *  @param stdinSizeDiff an integer used to check the number of operands
    *  @param invalidOptions the list of invalid options
    *  @param appName the spark application name
    *  @return the number of options which check is not successful (0 if all options are ok)
    */
  def checkOptions(
                    paramOptions: OptionMap,
                    stdinSizeDiff: Int,
                    invalidOptions: List[String],
                    appName: String): Int = {
    if (invalidOptions.size > 0) {
      invalidOptions.foreach({ option =>
        println(appName + ": unrecognized option: '" + option + "'")
      })
    }
    if (stdinSizeDiff > 0) {
      println(appName + ": too many operands")
    }

    invalidOptions.size + stdinSizeDiff
  }

  /**
    * Parse the command line arguments
    *
    *  @param args the command line argument list
    *  @param paramOptionNames the command line parameter option name list
    *  @param paramNoargOptionNames the command line parameter option without argument name list
    *  @param stdinOptionNames the command line input option name list
    *  @return the options map containing all the command line arguments
    */
  def parseCmdLine(
                    args: Array[String],
                    paramOptionNames: List[Symbol],
                    paramNoargOptionNames: List[Symbol] = List(),
                    stdinOptionNames: List[Symbol] = List()): OptionMap = {
    val defaultOptionNames = List("log_dir")
    val optionRegex =
      "--(" +
        (paramOptionNames.map(a => a.name) ++ defaultOptionNames).mkString("|") +
        ")"
    val optionNoArgRegex = "--(" + paramNoargOptionNames.map(a => a.name).mkString("|") + ")"
    val defaultOptions = Map(
      'invalid    -> List(),
      'stdin      -> List()
    )
    val options = readArgument(optionRegex.r, optionNoArgRegex.r, args.toList, defaultOptions)
    // keep only the last one if an option is defined several times
    val paramOptions = options.filterKeys(!List('stdin, 'invalid).contains(_)).
      map { case (a,b) => a -> b(0) }
    val status = checkOptions(
      paramOptions,
      options('stdin).size,
      options('invalid),
      ""
    )
    if (status > 0) throw new ScalaAppParameterException
    paramOptions
  }

  /**
    * Print application name and its parameters values
    *
    *  @param appName the spark application name
    *  @param options the options map containing all the command line arguments already parsed
    */
  def printApplicationInfos(appName: String, options: OptionMap): Unit = {
    println( "Application name: " + appName)
    options.foreach({ case (optionName, optionValue) =>
      println(optionName.name + ": " + optionValue)
    })
  }

  /**
    * Read a command line argument, and add it in the options map
    *
    *  @param optionRegex the regex matching all command line parameter options
    *  @param optionNoargRegex the regex matching all command line parameter options without argument
    *  @param args the remaining command line argument list
    *  @param options the options map containing all the command line arguments already parsed
    *  @return a new map containing the elements from options and the top option from args
    */
  def readArgument(
                    optionRegex: Regex,
                    optionNoargRegex: Regex,
                    args: List[String],
                    options: OptionListMap): OptionListMap = {
    val unknownOptionRegex = "(-.*)".r
    args match {
      case Nil =>
        options
      case optionRegex(name) :: value :: tail =>
        // pop (name -> value) from args, and add it in options(name) list
        readArgument(
          optionRegex,
          optionNoargRegex,
          tail,
          appendOption(Symbol(name), value, options)
        )
      case optionNoargRegex(name) :: tail =>
        // pop name from args, and add (name -> "true") in options(name) list
        readArgument(
          optionRegex,
          optionNoargRegex,
          tail,
          appendOption(Symbol(name), "true", options)
        )
      case unknownOptionRegex(value) :: tail =>
        // pop value from args, and add it in options('invalid) list
        readArgument(optionRegex, optionNoargRegex, tail, appendOption('invalid, value, options))
      case value :: tail =>
        // pop value from args, and add it in options('stdin) list
        readArgument(optionRegex, optionNoargRegex, tail, appendOption('stdin, value, options))
    }
  }

  case class ScalaAppParameterException() extends Exception
}
