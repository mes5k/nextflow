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

package nextflow.file
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
/**
 *  Helper class used to aggregate values having the same key
 *  to files
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class SimpleFileCollector extends FileCollector {

    protected ConcurrentMap<String,Path> cache = new ConcurrentHashMap<>()

    SimpleFileCollector( ) {

    }

    protected Path _file( String name ) {
        (Path)cache.getOrCreate(name) {
            def result = Files.createFile(getTempDir().resolve(name))
            if( seed instanceof Map && ((Map)seed).containsKey(name)) {
                append0(result, normalizeToStream(((Map)seed).get(name)))
            }
            else if( seed ) {
                append0(result, normalizeToStream(seed))
            }
            return result
        }
    }

    @Override
    SimpleFileCollector add( String key, value ) {
        append0( _file(key), normalizeToStream(value))
        return this
    }


    /**
     * Append the content of a file to the target file having {@code key} as name
     *
     * @param key
     * @param fileToAppend
     */
    protected void append0( Path source, InputStream stream ) {
        int n
        byte[] buffer = new byte[10 * 1024]
        def output = Files.newOutputStream(source, APPEND)

        try {
            while( (n=stream.read(buffer)) > 0 ) {
                output.write(buffer,0,n)
            }
            // append the new line separator
            if( newLine )
                output.write( System.lineSeparator().bytes )
        }
        finally {
            stream.closeQuietly()
            output.closeQuietly()
        }
    }

    /**
     *
     * @return The number of files in the appender accumulator
     */
    int size() {
        cache.size()
    }

    boolean isEmpty() {
        cache.isEmpty()
    }

    boolean containsKey(String key) {
        return cache.containsKey(key)
    }

    Path get(String name) {
        cache.get(name)
    }

    List<Path> getFiles() {
        new ArrayList<Path>(cache.values())
    }

    /**
     * {@inheritDoc}
     */
    @Override
    void saveFile( Closure<Path> closure ) {

        def result = []
        Iterator<Path> itr = cache.values().iterator()
        while( itr.hasNext() ) {
            def item = itr.next()
            def target = closure.call(item.getName())
            result << Files.move(item, target, StandardCopyOption.REPLACE_EXISTING)
            itr.remove()
        }

    }


}