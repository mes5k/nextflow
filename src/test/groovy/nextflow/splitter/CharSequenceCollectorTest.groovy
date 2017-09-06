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

package nextflow.splitter

import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class CharSequenceCollectorTest extends Specification {

    def 'test string buffer' () {

        given:
        def buffer = new CharSequenceCollector()
        when:
        buffer.add('alpha')
        buffer.add('-')
        buffer.add('gamma')
        buffer.add('-')
        buffer.add('delta')
        buffer.add(null)
        buffer.add('')

        then:
        buffer.toString() == 'alpha-gamma-delta'

    }

    def 'test is empty' () {

        when:
        def buffer = new CharSequenceCollector()
        then:
        !buffer.hasChunk()

        when:
        buffer.add('hello')
        then:
        buffer.hasChunk()

        when:
        buffer.next()
        then:
        !buffer.hasChunk()

    }

    def 'test reset' () {

        given:
        def buffer = new CharSequenceCollector()
        buffer.add('hello')

        when:
        buffer.next()
        then:
        !buffer.hasChunk()
        buffer.toString() == ''
    }

}
