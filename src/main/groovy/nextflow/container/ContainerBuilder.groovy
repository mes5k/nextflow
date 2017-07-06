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

import java.nio.file.Path

import groovy.transform.PackageScope
import nextflow.util.Escape
import nextflow.util.MemoryUnit
import nextflow.util.PathTrie
/**
 * Base class for container wrapper builders
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
abstract class ContainerBuilder {

    final protected List env = []

    final protected List<Path> mounts = []

    protected List<String> runOptions = []

    protected List<String> engineOptions = []

    protected String cpus

    protected String memory

    protected String temp

    protected String image

    protected Path workDir

    protected boolean readOnlyInputs


    ContainerBuilder addRunOptions(String str) {
        runOptions.add(str)
        return this
    }

    ContainerBuilder addEngineOptions(String str) {
        engineOptions.add(str)
        return this
    }

    ContainerBuilder setCpus( String value ) {
        this.cpus = value
        return this
    }

    ContainerBuilder setMemory( value ) {
        if( value instanceof MemoryUnit )
            this.memory = "${value.toMega()}m"

        else if( value instanceof String )
            this.memory = value

        else
            throw new IllegalArgumentException("Not a supported memory value")

        return this
    }

    ContainerBuilder setWorkDir( Path path ) {
        this.workDir = path
        return this
    }

    ContainerBuilder setName(String name) {

    }

    ContainerBuilder setTemp( String value ) {
        this.temp = value
        return this
    }

    abstract ContainerBuilder params( Map config )

    abstract ContainerBuilder build(StringBuilder result)

    abstract String getRunCommand()

    String getKillCommand() { return null }

    String getRemoveCommand() { return null }

    StringBuilder appendHelpers( StringBuilder wrapper ) {
        return wrapper
    }

    StringBuilder appendRunCommand( StringBuilder wrapper ) {
        wrapper << runCommand
    }

    ContainerBuilder build() {
        build(new StringBuilder())
    }

    ContainerBuilder addEnv( entry ) {
        env.add(entry)
        return this
    }

    ContainerBuilder addMount( Path path ) {
        if( path )
            mounts.add(path)
        return this
    }

    ContainerBuilder addMountForInputs( Map<String,Path> inputFiles ) {

        mounts.addAll( inputFilesToPaths(inputFiles) )
        return this
    }

    @PackageScope
    static List<Path> inputFilesToPaths( Map<String,Path> inputFiles ) {

        def List<Path> files = []
        inputFiles.each { name, storePath ->

            def path = storePath.getParent()
            if( path ) files << path

        }
        return files

    }

    /**
     * Given a normalised shell script (starting with a she-bang line)
     * replace the first token on the first line with a docker run command
     *
     * @param script
     * @param docker
     * @return
     */
    String addContainerRunCommand( ContainerScriptTokens script ) {

        final result = new ArrayList<String>(script.lines)
        final i = script.index
        final main = result[i].trim()
        final p = main.indexOf(' ')
        result[i] = ( p != -1
                ? runCommand + main.substring(p)
                : runCommand )
        result.add('')
        return result.join('\n')
    }

    /**
     * Get the env command line option for the give environment definition
     *
     * @param env
     * @param result
     * @return
     */
    protected CharSequence makeEnv( env, StringBuilder result = new StringBuilder() ) {
        // append the environment configuration
        if( env instanceof File ) {
            env = env.toPath()
        }
        if( env instanceof Path ) {
            result << '-e "BASH_ENV=' << Escape.path(env) << '"'
        }
        else if( env instanceof Map ) {
            short index = 0
            for( Map.Entry entry : env.entrySet() ) {
                if( index++ ) result << ' '
                result << ("-e \"${entry.key}=${entry.value}\"")
            }
        }
        else if( env instanceof String && env.contains('=') ) {
            result << '-e "' << env << '"'
        }
        else if( env ) {
            throw new IllegalArgumentException("Not a valid environment value: $env [${env.class.name}]")
        }

        return result
    }


    /**
     * Get the volumes command line options for the given list of input files
     *
     * @param mountPaths
     * @param binDir
     * @param result
     * @return
     */
    protected CharSequence makeVolumes(List<Path> mountPaths, StringBuilder result = new StringBuilder() ) {

        // add the work-dir to the list of container mounts
        final workDirStr = workDir?.toString()
        final allMounts = new ArrayList<Path>(mountPaths)
        if( workDir )
            allMounts << workDir

        // find the longest commons paths and mount only them
        final trie = new PathTrie()
        allMounts.each { trie.add(it) }

        // when mounts are read-only make sure to remove the work-dir path
        final paths = trie.longest()
        if( readOnlyInputs && workDirStr && paths.contains(workDirStr) )
            paths.remove(workDirStr)

        paths.each {
            if(it) {
                result << composeVolumePath(it,readOnlyInputs)
                result << ' '
            }
        }

        // when mounts are read-only, make sure to include the work-dir as writable
        if( readOnlyInputs && workDir ) {
            result << composeVolumePath(workDirStr)
            result << ' '
        }

        // -- append by default the current path -- this is needed when `scratch` is set to true
        result << composeVolumePath('$PWD')

        return result
    }

    protected String composeVolumePath( String path, boolean readOnly = false ) {
        def result = "-v ${escape(path)}:${escape(path)}"
        if( readOnly )
            result += ':ro'
        return result
    }

    protected String escape(String path) {
        path.startsWith('$') ? "\"$path\"" : Escape.path(path)
    }

}
