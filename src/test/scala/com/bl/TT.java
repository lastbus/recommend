package com.bl;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

/**
 * Created by MK33 on 2016/6/27.
 */
public class TT {

    Option logfile   = OptionBuilder.withArgName( "file" )
            .hasArg()
            .withDescription(  "use given file for log" )
            .create( "logfile" );

    Option logger    = OptionBuilder.withArgName("classname")
            .hasArg()
            .withDescription( "the class which it to perform "
                    + "logging" )
            .create( "logger" );
}
