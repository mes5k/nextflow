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

package nextflow.container
import groovy.transform.CompileStatic
/**
 * Helper methods to handle Docker containers
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class DockerBuilder extends ContainerBuilder {

    private boolean sudo

    private boolean remove = true

    private boolean userEmulation

    private String registry

    private String name

    private boolean tty

    private String entryPoint

    private static final String USER_AND_HOME_EMULATION = '-u $(id -u) -e "HOME=${HOME}" -v /etc/passwd:/etc/passwd:ro -v /etc/shadow:/etc/shadow:ro -v /etc/group:/etc/group:ro -v $HOME:$HOME'

    private String runCommand

    private String removeCommand

    private String killCommand

    private kill = true

    private boolean legacy

    DockerBuilder( String name ) {
        this.image = name
    }

    @Override
    DockerBuilder params( Map params ) {
        if( !params ) return this

        if( params.containsKey('temp') )
            this.temp = params.temp

        if( params.containsKey('engineOptions') )
            addEngineOptions(params.engineOptions.toString())

        if( params.containsKey('runOptions') )
            addRunOptions(params.runOptions.toString())

        if ( params.containsKey('userEmulation') )
            this.userEmulation = params.userEmulation?.toString() == 'true'

        if ( params.containsKey('remove') )
            this.remove = params.remove?.toString() == 'true'

        if( params.containsKey('sudo') )
            this.sudo = params.sudo?.toString() == 'true'

        if( params.containsKey('tty') )
            this.tty = params.tty?.toString() == 'true'

        if( params.containsKey('entry') )
            this.entryPoint = params.entry

        if( params.containsKey('kill') )
            this.kill = params.kill

        if( params.containsKey('legacy') )
            this.legacy = params.legacy?.toString() == 'true'

        if( params.containsKey('readOnlyInputs') )
            this.readOnlyInputs = params.readOnlyInputs?.toString() == 'true'

        return this
    }

    @Override
    DockerBuilder setName( String name ) {
        this.name = name
        return this
    }

    @Override
    DockerBuilder build(StringBuilder result) {
        assert image

        if( sudo )
            result << 'sudo '

        result << 'docker '

        if( engineOptions )
            result << engineOptions.join(' ') << ' '

        result << 'run -i '

        if( cpus ) {
            if( legacy )
                result << "--cpuset ${cpus} "
            else
                result << "--cpuset-cpus ${cpus} "
        }

        if( memory )
            result << "--memory ${memory} "

        if( tty )
            result << '-t '

        // add the environment
        for( def entry : env ) {
            result << makeEnv(entry) << ' '
        }

        if( temp )
            result << "-v $temp:/tmp "

        if( userEmulation )
            result << USER_AND_HOME_EMULATION << ' '

        // mount the input folders
        result << makeVolumes(mounts)
        result << ' -w "$PWD" '

        if( entryPoint )
            result << '--entrypoint ' << entryPoint << ' '

        if( runOptions )
            result << runOptions.join(' ') << ' '

        // the name is after the user option so it has precedence over any options provided by the user
        if( name )
            result << '--name ' << name << ' '

        if( registry )
            result << registry

        // finally the container name
        result << image

        // return the run command as result
        runCommand = result.toString()

        // use an explicit 'docker rm' command since the --rm flag may fail. See https://groups.google.com/d/msg/docker-user/0Ayim0wv2Ls/tDC-tlAK03YJ
        if( remove && name ) {
            removeCommand = 'docker rm ' + name
            if( sudo ) removeCommand = 'sudo ' + removeCommand
        }

        if( kill )  {
            killCommand = 'docker kill '
            // if `kill` is a string it is interpreted as a the kill signal
            if( kill instanceof String ) killCommand += "-s $kill "
            killCommand += name
            // prefix with sudo if required
            if( sudo ) killCommand = 'sudo ' + killCommand
        }


        return this
    }


    /**
     * @return The command string to run a container from Docker image
     */
    String getRunCommand() { runCommand }

    /**
     * @return The command string to remove a container
     */
    @Override
    String getRemoveCommand() { removeCommand }

    /**
     * @return The command string to kill a running container
     */
    @Override
    String getKillCommand() { killCommand }

    static boolean isAbsoluteDockerName(String image) {
        def p = image.indexOf('/')
        if( p==-1 )
            return false

        image = image.substring(0,p)
        image.contains('.') || image.contains(':')
    }

    static String normalizeImageName( String imageName, Map containerConf ) {

        if( !imageName )
            return null

        String reg = containerConf?.registry
        if( !reg )
            return imageName

        if( isAbsoluteDockerName(imageName) )
            return imageName

        if( !reg.endsWith('/') )
            reg += '/'

        return reg + imageName
    }


}
