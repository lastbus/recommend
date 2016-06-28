package com.bl

import org.apache.commons.cli.{Option, Options}

/**
 * Created by MK33 on 2016/6/27.
 */
object CommandCli {


  def main(args: Array[String]) = {
    val options = new Options

    val help = new Option("help", "print this message" )
    val projecthelp = new Option( "projecthelp", "print project help information" )
    val version = new Option( "version", "print the version information and exit" )
    val quiet = new Option( "quiet", "be extra quiet" )
    val verbose = new Option( "verbose", "be extra verbose" )
    val debug = new Option( "debug", "print debugging information" )
    val emacs = new Option( "emacs", "produce logging information without adornments" )

    options.addOption(help)
    options.addOption(projecthelp)
    options.addOption(version)
    options.addOption(quiet)
    options.addOption(verbose)
    options.addOption(debug)
    options.addOption(emacs)
    options





  }

}
