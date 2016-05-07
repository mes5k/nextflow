package nextflow.dag

import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class DAGTest extends Specification {

    def beforeSpec() {
        Vertex.reset()
    }

    def 'should add new vertices' () {

        given:
        def proc1 = new Object()
        def proc2 = new Object()
        def ch1 = new Object()
        def ch2 = new Object()
        def ch3 = new Object()

        def v1=null
        def v2=null

        def dag = new DAG()
        when:
        dag.addVertex(new VertexHandler(
                instance: proc1,
                label: 'Process 1',
                inbounds: [ new ChannelHandler(instance: ch1, label: 'Channel 1') ],
                outbounds: [ new ChannelHandler(instance: ch2, label: 'Channel 2') ],
                type: VertexType.PROCESS
        ))

        v1 = dag.vertices[0]

        then:
        dag.vertices.size() == 1
        v1.instance.is(proc1)
        v1.label == 'Process 1'
        v1.index == 0

        dag.edges.size() == 2

        dag.edges[0].label == 'Channel 1'
        dag.edges[0].instance .is ch1
        dag.edges[0].from == null
        dag.edges[0].to == v1

        dag.edges[1].label == 'Channel 2'
        dag.edges[1].instance .is ch2
        dag.edges[1].from == v1
        dag.edges[1].to == null

        when:
        dag.addVertex( new VertexHandler(
                instance: proc2,
                label: 'Process 2',
                inbounds: [ new ChannelHandler(instance: ch2) ],
                outbounds: [ new ChannelHandler(instance: ch3, label: 'Channel 3') ],
                type: VertexType.PROCESS
        ))

        v1 = dag.vertices[0]
        v2 = dag.vertices[1]
        then:
        dag.vertices.size() == 2
        v1.instance.is(proc1)
        v1.label == 'Process 1'
        v1.index == 0

        v2.instance.is(proc2)
        v2.label == 'Process 2'
        v2.index == 1

        dag.edges.size() == 3

        dag.edges[0].label == 'Channel 1'
        dag.edges[0].instance .is ch1
        dag.edges[0].from == null
        dag.edges[0].to == v1

        dag.edges[1].label == 'Channel 2'
        dag.edges[1].instance .is ch2
        dag.edges[1].from == v1
        dag.edges[1].to == v2

        dag.edges[2].label == 'Channel 3'
        dag.edges[2].instance .is ch3
        dag.edges[2].from == v2
        dag.edges[2].to == null


        when:
        dag.addVertex( new VertexHandler(
                instance: new Object(),
                label: 'Process 3',
                inbounds: [ new ChannelHandler(instance: ch2) ],
                outbounds: [],
                type: VertexType.PROCESS
        ))
        then:
        thrown( DuplicateInputEdgeException )

        when:
        dag.addVertex( new VertexHandler(
                instance: new Object(),
                label: 'Process 3',
                inbounds: [],
                outbounds: [new ChannelHandler(instance: ch2)],
                type: VertexType.PROCESS
        ))
        then:
        thrown( DuplicateOutputEdgeException )
    }


}
