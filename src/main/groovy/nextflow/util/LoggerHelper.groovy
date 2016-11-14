/*
 * Copyright (c) 2013-2016, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2016, Paolo Di Tommaso and the respective authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package nextflow.util
import static nextflow.Const.MAIN_PACKAGE
import static nextflow.Const.S3_UPLOADER_CLASS

import java.lang.reflect.Field
import java.nio.file.NoSuchFileException
import java.util.concurrent.atomic.AtomicBoolean

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.ThrowableProxy
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.CoreConstants
import ch.qos.logback.core.FileAppender
import ch.qos.logback.core.LayoutBase
import ch.qos.logback.core.encoder.LayoutWrappingEncoder
import ch.qos.logback.core.filter.Filter
import ch.qos.logback.core.joran.spi.NoAutoStart
import ch.qos.logback.core.rolling.FixedWindowRollingPolicy
import ch.qos.logback.core.rolling.RollingFileAppender
import ch.qos.logback.core.rolling.TriggeringPolicyBase
import ch.qos.logback.core.spi.FilterReply
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import nextflow.Global
import nextflow.Session
import nextflow.cli.CliOptions
import nextflow.cli.Launcher
import nextflow.exception.AbortOperationException
import nextflow.exception.ProcessException
import nextflow.file.FileHelper
import org.apache.commons.lang.exception.ExceptionUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;


/**
 * Helper class to setup the logging subsystem
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class LoggerHelper {

    static private String STARTUP_ERROR = 'startup failed:\n'

    static private String logFileName

    static private String hashFileName = 'hashes'

    private CliOptions opts

    private boolean rolling = false

    private boolean daemon = false

    private int minIndex = 1

    private int maxIndex = 9

    private Map<String,Level> packages = [:]

    private LoggerContext loggerContext

    private ConsoleAppender consoleAppender

    private FileAppender fileAppender

    private FileAppender hashAppender

    public static final String hashMarkerString = "HASHES"

    public static final Marker hashMarker = MarkerFactory.getMarker(hashMarkerString)

    LoggerHelper setRolling( boolean value ) {
        this.rolling = value
        return this
    }

    LoggerHelper setDaemon( boolean value ) {
        this.daemon = value
        return this
    }

    LoggerHelper setMinIndex( int value ) {
        this.minIndex = value
        return this
    }

    LoggerHelper setMaxIndex( int value ) {
        this.maxIndex = value
        return this
    }

    LoggerHelper(CliOptions opts) {
        this.opts = opts
        this.loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory()
    }

    void setup() {
        logFileName = opts.logFile

        def hashFilter = new MarkerLoggerFilter()
        hashFilter.start()

        final boolean quiet = opts.quiet
        final List<String> debugConf = opts.debug ?: new ArrayList<String>()
        final List<String> traceConf = opts.trace ?: ( System.getenv('NXF_TRACE')?.tokenize(', ') ?: new ArrayList<String>())

        // Reset all the logger
        final root = loggerContext.getLogger('ROOT')
        root.detachAndStopAllAppenders()

        // -- define the console appender
        packages[MAIN_PACKAGE] = quiet ? Level.WARN : Level.INFO

        // -- add the S3 uploader by default
        if( !debugConf.contains(S3_UPLOADER_CLASS) && !traceConf.contains(S3_UPLOADER_CLASS) ) {
            debugConf << S3_UPLOADER_CLASS
        }

        debugConf.each { packages[it] = Level.DEBUG }
        traceConf.each { packages[it] = Level.TRACE }

        // -- the console appender
        this.consoleAppender = createConsoleAppender()

        // -- the file appenders
        this.fileAppender = rolling ? createRollingAppender(logFileName, null)
                                    : createFileAppender(logFileName, null)

        this.hashAppender = rolling ? createRollingAppender(hashFileName, hashFilter)
                                    : createFileAppender(hashFileName, hashFilter)

        // -- configure the ROOT logger
        root.setLevel(Level.INFO)
        if( fileAppender )
            root.addAppender(fileAppender)
        if( consoleAppender )
            root.addAppender(consoleAppender)
        if( hashAppender )
            root.addAppender(hashAppender)

        // -- main package logger
        def mainLevel = packages[MAIN_PACKAGE] == Level.TRACE ? Level.TRACE : Level.DEBUG
        def logger = createLogger(MAIN_PACKAGE, mainLevel)

        // -- set AWS lib level to WARN to reduce noise in the log file
        final AWS = 'com.amazonaws'
        if( !debugConf.contains(AWS) && !traceConf.contains(AWS)) {
            createLogger(AWS, Level.WARN)
        }

        // -- debug packages specified by the user
        debugConf.each { String clazz -> createLogger(clazz, Level.DEBUG) }
        // -- trace packages specified by the user
        traceConf.each { String clazz -> createLogger(clazz, Level.TRACE) }

        if(!consoleAppender)
            logger.debug "Console appender: disabled"
    }

    protected Logger createLogger(String clazz, Level level ) {
        def logger = loggerContext.getLogger( clazz )
        logger.setLevel(level)
        logger.setAdditive(false)
        if( fileAppender )
            logger.addAppender(fileAppender)
        if( consoleAppender )
            logger.addAppender(consoleAppender)
        if( hashAppender )
            logger.addAppender(hashAppender)

        return logger
    }

    protected ConsoleAppender createConsoleAppender() {

        final ConsoleAppender result = daemon && opts.isBackground() ? null : new ConsoleAppender()
        if( result )  {

            final filter = new ConsoleLoggerFilter( packages )
            filter.setContext(loggerContext)
            filter.start()

            result.setContext(loggerContext)
            result.setEncoder( new LayoutWrappingEncoder( layout: new PrettyConsoleLayout() ) )
            result.addFilter(filter)
            result.start()
        }

        return result
    }

    protected RollingFileAppender createRollingAppender(String name, Filter filter) {

        RollingFileAppender result = name ? new RollingFileAppender() : null
        if( result ) {
            result.file = name

            def rollingPolicy = new  FixedWindowRollingPolicy( )
            rollingPolicy.fileNamePattern = "${name}.%i"
            rollingPolicy.setContext(loggerContext)
            rollingPolicy.setParent(result)
            rollingPolicy.setMinIndex(1)
            rollingPolicy.setMaxIndex(9)
            rollingPolicy.start()

            result.rollingPolicy = rollingPolicy
            result.encoder = createEncoder()
            result.setContext(loggerContext)
            result.setTriggeringPolicy(new RollOnStartupPolicy())
            result.triggeringPolicy.start()

            if ( filter )
                result.addFilter(filter)

            result.start()
        }

        return result
    }

    protected FileAppender createFileAppender(String name, Filter filter) {

        FileAppender result = name ? new FileAppender() : null
        if( result ) {
            result.file = name
            result.encoder = createEncoder()
            result.setContext(loggerContext)

            if ( filter )
                result.addFilter(filter)

            result.start()
        }

        return result
    }

    protected PatternLayoutEncoder createEncoder() {
        def result = new PatternLayoutEncoder()
        result.setPattern('%d{MMM-dd HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n')
        result.setContext(loggerContext)
        result.start()
        return result
    }

    /**
     * Configure the underlying logging subsystem. The logging information are saved
     * in a file named '.nextflow.log' in the current path, plus to the terminal console.
     * <p>
     *     By default only the INFORMATION level logging messages are visualized to the console,
     *     instead in the file are saved the DEBUG level messages.
     *
     * @param logFileName The file where save the application log
     * @param quiet When {@code true} only Warning and Error messages are visualized to teh console
     * @param debugConf The list of packages for which use a Debug logging level
     * @param traceConf The list of packages for which use a Trace logging level
     */

    static void configureLogger( Launcher launcher ) {
        new LoggerHelper(launcher.options)
                .setDaemon(launcher.isDaemon())
                .setRolling(true)
                .setup()
    }

    static void configureLogger( final CliOptions opts, boolean daemon = false ) {
        new LoggerHelper(opts)
                .setDaemon(daemon)
                .setRolling(true)
                .setup()
    }

    /*
     * Filters the logging event based on the level assigned to a specific 'package'
     */
    static class ConsoleLoggerFilter extends Filter<ILoggingEvent> {

        private final Map<String,Level> allLevels
        private final Set<String> packages
        private final int len

        ConsoleLoggerFilter( Map<String,Level> levels )  {
            this.allLevels = levels
            this.packages = levels.keySet()
            this.len = packages.size()
        }

        @Override
        FilterReply decide(ILoggingEvent event) {

            if (!isStarted()) {
                return FilterReply.NEUTRAL;
            }

            def logger = event.getLoggerName()
            def level = event.getLevel()
            for( int i=0; i<len; i++ ) {
                final key=packages[i]
                if ( logger.startsWith(key) && level.isGreaterOrEqual(Level.INFO) && level.isGreaterOrEqual(allLevels[key]) ) {
                    return FilterReply.NEUTRAL
                }
            }

            return FilterReply.DENY
        }
    }

    static class MarkerLoggerFilter extends Filter<ILoggingEvent> {

        @Override
        FilterReply decide(ILoggingEvent event) {

            if (event.getMarker() == hashMarker) {
                return FilterReply.NEUTRAL;
            }

            return FilterReply.DENY
        }
    }

    /*
     * Do not print INFO level prefix, used to print logging information
     * to the application stdout
     */
    static class PrettyConsoleLayout extends LayoutBase<ILoggingEvent> {

        public String doLayout(ILoggingEvent event) {
            StringBuilder buffer = new StringBuilder(128);
            if( event.level == Level.INFO ) {
                buffer .append(event.getFormattedMessage()) .append(CoreConstants.LINE_SEPARATOR)
            }

            else if( event.level == Level.ERROR ) {
                def error = ( event.getThrowableProxy() instanceof ThrowableProxy
                            ? (event.getThrowableProxy() as ThrowableProxy).throwable
                            : null) as Throwable

                appendFormattedMessage(buffer, event, error, (Session)Global.session)
            }
            else {
                buffer
                        .append( event.getLevel().toString() ) .append(": ")
                        .append(event.getFormattedMessage())
                        .append(CoreConstants.LINE_SEPARATOR)
            }

            return buffer.toString()
        }
    }


    /**
     * Find out the script line where the error has thrown
     */
    static void appendFormattedMessage( StringBuilder buffer, ILoggingEvent event, Throwable fail, Session session) {

        final scriptName = session?.scriptName
        final className = session?.scriptClassName
        final message = event.getFormattedMessage()
        final quiet = fail instanceof AbortOperationException || fail instanceof ProcessException
        List error = fail ? findErrorLine(fail, className) : null
        final normalize = { String str -> str ?. replace("${className}.", '')}

        // error string is not shown for abort operation
        if( !quiet ) {
            buffer.append("ERROR ~ ")
        }

        // add the log message
        if( fail instanceof MissingPropertyException ) {
            def name = fail.property ?: getDetailMessage(fail)
            buffer.append("No such variable: ${name}")
        }
        else if( fail instanceof NoSuchFileException ) {
            buffer.append("No such file: ${normalize(fail.message)}")
        }
        else if( message && message.startsWith(STARTUP_ERROR))  {
            buffer.append(formatStartupErrorMessage(message))
        }
        else if( message && !message.startsWith('@') ) {
            buffer.append(normalize(message))
        }
        else if( fail ) {
            buffer.append( normalize(fail.message) ?: "Unexpected error [${fail.class.simpleName}]" )
        }
        else {
            buffer.append("Unexpected error")
        }
        buffer.append(CoreConstants.LINE_SEPARATOR)
        buffer.append(CoreConstants.LINE_SEPARATOR)

        // extra formatting
        if( error ) {
            buffer.append(" -- Check script '${scriptName}' at line: ${error[1]} or see '${logFileName}' file for more details")
            buffer.append(CoreConstants.LINE_SEPARATOR)
        }
        else if( logFileName && !quiet ) {
            buffer.append(" -- Check '${logFileName}' file for details")
            buffer.append(CoreConstants.LINE_SEPARATOR)
        }

    }

    @PackageScope
    static String formatStartupErrorMessage( String message ) {
        message.replace(STARTUP_ERROR,'').replaceFirst(/^_nf_script_[a-z0-9]+: *[0-9]+: */,'')
    }

    static String getDetailMessage(Throwable error) {
        try {
            def clazz = error.class
            while(  clazz != Throwable && clazz )
                clazz = clazz.getSuperclass()
            Field field = clazz.getDeclaredField('detailMessage')
            field.setAccessible(true)
            return field.get(error)
        }
        catch( Throwable e ) {
            return null
        }
    }

    static @PackageScope List<String> findErrorLine( Throwable e, String scriptName ) {
        def lines = ExceptionUtils.getStackTrace(e).split('\n')
        List error = null
        for( String str : lines ) {
            if( (error=getErrorLine(str,scriptName))) {
                break
            }
        }
        return error
    }

    @PackageScope
    static List<String> getErrorLine( String line, String scriptName = null) {
        if( scriptName==null )
            scriptName = /.+\.nf/

        def pattern = ~/.*\(($scriptName):(\d*)\).*/
        def m = pattern.matcher(line)
        return m.matches() ? [m.group(1), m.group(2)] : null
    }

    /**
     * Trigger a new log file on application start-up
     *
     * See here http://stackoverflow.com/a/2647471/395921
     */
    @NoAutoStart
    static class RollOnStartupPolicy<E> extends TriggeringPolicyBase<E> {

        private final AtomicBoolean firstTime = new AtomicBoolean(true);

        @Override
        public boolean isTriggeringEvent(File activeFile, E event) {

            if ( !firstTime.get() ) { // fast path
                return false;
            }

            if (firstTime.getAndSet(false) && !FileHelper.empty(activeFile) ) {
                return true;
            }
            return false;
        }

    }


}
