package nextflow.dag

import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class VertexTest extends Specification {

    def 'should create a vertex' () {

        when:
        def v1 = new Vertex(10, 'Label A')
        def v2 = new Vertex(20, 'Label B')

        then:
        v1.instance == 10
        v1.label == 'Label A'
        v2.instance == 20
        v2.label == 'Label B'
        v2.index == v1.index +1

    }


}
