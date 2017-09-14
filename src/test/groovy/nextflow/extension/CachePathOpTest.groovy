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

import groovyx.gpars.dataflow.DataflowQueue
import nextflow.Channel
import nextflow.Session
import test.TestHelper
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 * @author Mike Smoot <mes@aescon.com>
 */
class CachePathOpTest extends Specification {

    def setupSpec() {
        new Session()
    }

    def testCachePathFile() {
        setup:
        def f1 = TestHelper.createInMemTempFile("f1")

        when:
        def ch1 = Channel.fromPath(f1).cachePath()

        then:
        f1 != ch1.val
    }

//    def testCachePathDir() {
//        setup:
//        def f1 = new FakeFile()
//        def f2 = new FakeFile()
//        def d1 = new FakeDir()
//        def ch1 = Channel.fromPath(d1)
//
//        when:
//        result = CachePathOp.
//
//        then:
//        result.ls() == [f1, f2]
//
//    }

}
