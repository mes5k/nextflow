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

package nextflow.extension
import static nextflow.util.CheckHelper.checkParams

import java.nio.file.Path
import java.nio.file.Files
import static java.nio.file.StandardCopyOption.*

import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowReadChannel
import nextflow.Channel
import nextflow.Global
import nextflow.file.FileHelper
import nextflow.file.CopyMoveHelper
import nextflow.util.CacheHelper
import nextflow.Nextflow

/**
 * Implements the body of {@link DataflowExtensions#cachePath(groovyx.gpars.dataflow.DataflowReadChannel)} operator
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 * @author Mike Smoot <mes@aescon.com>
 */
@Slf4j
class CachePathOp {

    static final Map CACHE_PATH_PARAMS = [
            storeDir: [Path,File,CharSequence]
    ]

    private final Map params

    private DataflowQueue result

    private DataflowReadChannel channel

    private Path storeDir

    private boolean linkLocalPaths = true

    CachePathOp( final DataflowReadChannel channel, Map params) {

        checkParams('cachePath', params, CACHE_PATH_PARAMS)
        this.params = params
        this.channel = channel
        this.result = new DataflowQueue()
    }

    /*
     * Each time a value is received, check that it's a Path or File,
     * cache as directed, and then return the cached path.
     */
    protected processPath( inPath ) {
        if( inPath instanceof Path || inPath instanceof File ) {
            result.bind( getCachedPath( inPath ) )
        } else {
            throw new IllegalArgumentException("Can only cache files or directories!")
        }
    }

    protected getCachedPath( inPath ) {
        def cached = null
        def storeDir = params?.storeDir ? params?.storeDir as Path : null

        if ( Files.isDirectory( inPath ) ) {
            log.warn("caching dir")
            cached = Nextflow.cacheableDir( inPath, storeDir )
            if ( FileHelper.empty( cached ) ) {
                log.warn("copying dir")
                CopyMoveHelper.copyDirectory( inPath, cached, REPLACE_EXISTING )
            }
        } else {
            log.warn("caching file")
            cached = Nextflow.cacheableFile( inPath.getParent(), inPath.getFileName().toString(), storeDir )
            if ( !Files.exists( cached ) ) {
                log.warn("copying dir")
                CopyMoveHelper.copyFile( inPath, cached, false )
            }
        }

        return cached
    }

    protected finish(obj) {
        result.bind(Channel.STOP)
    }

    DataflowQueue apply() {
        DataflowHelper.subscribeImpl( channel, [onNext: this.&processPath, onComplete: this.&finish] )
        return result
    }
}
