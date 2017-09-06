/*
 * Copyright (c) 2013-2017, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2017, Paolo Di Tommaso and the respective authors.
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

package nextflow
import java.nio.file.Path
import java.nio.file.Paths

import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.util.Duration
import nextflow.util.IniFile
import nextflow.util.MemoryUnit
import org.apache.commons.lang.StringUtils
/**
 * Hold global variables
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class Global {

    /**
     * The pipeline session instance
     */
    static private ISession session

    /**
     * The main configuration object
     */
    static Map config

    /**
     * @return The object instance representing the current session
     */
    static ISession getSession() {
        session
    }

    /**
     * Set the application session object
     *
     * @param value An object instance representing the current session
     */
    static void setSession( ISession value ) {
        session = value
    }

    /**
     * Retrieve the AWS credentials from the given context. It look for AWS credential in the following order
     * 1) Nextflow config {@code aws.accessKey} and {@code aws.secretKey} pair
     * 2) System env {@code AWS_ACCESS_KEY} and {@code AWS_SECRET_KEY} pair
     * 3) System env {@code AWS_ACCESS_KEY_ID} and {@code AWS_SECRET_ACCESS_KEY} pair
     *
     *
     * @param env The system environment map
     * @param config The nextflow config object map
     * @return A pair where the first element is the access key and the second the secret key or
     *      {@code null} if the credentials are missing
     */
    @PackageScope
    static List<String> getAwsCredentials0( Map env, Map config, List<Path> files = []) {

        String a
        String b

        if( config && config.aws instanceof Map ) {
            a = ((Map)config.aws).accessKey
            b = ((Map)config.aws).secretKey

            if( a && b ) {
                log.debug "Using AWS credentials defined in nextflow config file"
                return [a, b]
            }

        }

        // as define by amazon doc
        // http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html
        if( env && (a=env.AWS_ACCESS_KEY_ID) && (b=env.AWS_SECRET_ACCESS_KEY) )  {
            log.debug "Using AWS credentials defined by environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
            return [a, b]
        }

        if( env && (a=env.AWS_ACCESS_KEY) && (b=env.AWS_SECRET_KEY) ) {
            log.debug "Using AWS credentials defined by environment variables AWS_ACCESS_KEY and AWS_SECRET_KEY"
            return [a, b]
        }

        for( Path it : files ) {
            final conf = new IniFile(it)
            if( (a=conf.section('default').aws_access_key_id) && (b=conf.section('default').aws_secret_access_key) ) {
                log.debug "Using AWS credential defined in `default` section in file: ${conf.file}"
                return [a,b]
            }
        }

        return null
    }

    static List<String> getAwsCredentials(Map env, Map config) {

        def home = Paths.get(System.properties.get('user.home') as String)
        def files = [ home.resolve('.aws/credentials'), home.resolve('.aws/config') ]
        getAwsCredentials0(env, config, files)

    }

    static String getAwsRegion(Map env=null, Map config=null) {
        if( env==null ) env = System.getenv()
        if( config==null ) config = this.config

        // check nxf config file
        if( config && config.aws instanceof Map ) {
            def region = ((Map)config.aws).region
            if( region )
                return region.toString()
        }

        if( env && env.AWS_DEFAULT_REGION )  {
            return env.AWS_DEFAULT_REGION.toString()
        }

        def home = Paths.get(System.properties.get('user.home') as String)
        def file = home.resolve('.aws/config')
        if( !file.exists() ) {
            return null
        }

        def ini = new IniFile(file)
        return ini.section('default').region
    }

    static List<String> getAwsCredentials(Map env) {
        getAwsCredentials(env, config)
    }

    static List<String> getAwsCredentials() {
        getAwsCredentials(System.getenv(), config)
    }

    static Map<String,?> getAwsClientConfig() {
        if( config?.aws?.client instanceof Map ) {
            return normalizeAwsClientConfig(config.aws.client as Map)
        }

        return null
    }

    /**
     * Convert configuration keys from camel-case notation (nextflow) to underscore
     * separated notation expected by the AWS client
     *
     * @return A map object containing the AWS client configuration properties
     */
    static protected Map normalizeAwsClientConfig(Map<String,?> client) {

        normalizeMemUnit(client, 'uploadChunkSize');
        normalizeDuration(client, 'uploadRetrySleep');


        def result = [:]
        client.each { String name, value ->
            def newKey = name.isCamelCase() ? StringUtils.splitByCharacterTypeCamelCase(name).join('_').toLowerCase() : name
            result.put(newKey,value?.toString())
        }
        return result
    }

    static void normalizeMemUnit(Map<String,?> client, String key) {
        if( client.get(key) instanceof String ) {
            client.put(key, MemoryUnit.of((String)client.get(key)))
        }
        if( client.get(key) instanceof MemoryUnit ) {
            client.put(key, ((MemoryUnit)client.get(key)).toBytes())
        }
    }

    static void normalizeDuration(Map<String,?> client, String key)  {
        if( client.get(key) instanceof String ) {
            client.put(key, Duration.of((String)client.get(key)))
        }
        if( client.get(key) instanceof Duration ) {
            client.put(key, ((Duration)client.get(key)).toMillis())
        }
    }

    /**
     * Run the specified closure on application termination
     *
     * @param callback A closure to be executed on application shutdown
     */
    static void onShutdown(Closure callback) {
        hooks.add(callback)
    }

    static final private List<Closure> hooks = []

    static synchronized cleanUp() {
        for( Closure c : hooks ) {
            try {
                c.call()
            }
            catch( Exception e ) {
                log.debug("Error during on cleanup", e )
            }
        }
    }

}
