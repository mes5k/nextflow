package nextflow.dag

import groovy.transform.CompileStatic

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class DAG {

    List<Edge> edges = new ArrayList<>(50)

    List<Vertex> vertices = new ArrayList<>(50)


    public void addVertex( VertexHandler vertex ) {

        vertex.inbounds.each { ChannelHandler channel ->
            inbound( vertex, channel )
        }

        vertex.outbounds.each { ChannelHandler channel ->
            outbound( vertex, channel )
        }
    }

    void inbound( VertexHandler handler, ChannelHandler entering )  {

        final vertex = getOrCreateVertex(handler)

        def edge = findEdge(entering.instance)
        if( !edge ) {
            edges << new Edge(instance: entering.instance, to: vertex, label: entering.label)
        }
        else if( edge.to == null ) {
            edge.to = vertex
        }
        else {
            throw new DuplicateInputEdgeException()
        }
    }

    void outbound( VertexHandler handler, ChannelHandler leaving) {

        final vertex = getOrCreateVertex(handler)

        final edge = findEdge(leaving.instance)
        if( !edge ) {
            edges << new Edge(instance: leaving.instance, from: vertex, label: leaving.label)
        }
        else if( edge.from == null ) {
            edge.from = vertex
        }
        else {
            throw new DuplicateOutputEdgeException()
        }

    }

    Vertex getOrCreateVertex( VertexHandler operator ) {
        def vertex = vertices.find { v -> v.instance.is(operator.instance) }
        if( !vertex ) {
            vertices.add(vertex = new Vertex(operator.instance, operator.label))
        }
        return vertex
    }

    Edge findEdge( def channel ) {
        return edges.find { edge -> edge.instance.is(channel) }
    }

    String toString() {
        def result = []
        result << "digraph graphname {"
        edges.each { result << it.toString()  }
        result << "}"
        return result.join('\n')
    }

}