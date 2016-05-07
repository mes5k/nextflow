package nextflow.dag

import groovy.transform.PackageScope
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import nextflow.Session

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class DAG {

    static enum VertexType {
        PROCESS,
        OPERATOR,
        ORIGIN,
        TERMINATION
    }

    List<Edge> edges = new ArrayList<>(50)

    List<Vertex> vertices = new ArrayList<>(50)

    private Session session

    public void addVertex( GraphEvent handler ) {

        def vertex = createVertex( handler.type, handler.label)

        handler.inbounds?.each { ChannelHandler channel ->
            inbound( vertex, channel )
        }

        handler.outbounds?.each { ChannelHandler channel ->
            outbound( vertex, channel )
        }
    }

    @PackageScope
    Vertex createVertex( VertexType type, String label ) {
        def result = new Vertex(type, label)
        vertices << result
        return result
    }

    private void inbound( Vertex vertex, ChannelHandler entering )  {

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

    private void outbound( Vertex vertex, ChannelHandler leaving) {

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

    Edge findEdge( def channel ) {
        return edges.find { edge -> edge.instance.is(channel) }
    }

    int indexOf(Vertex v) {
        vertices.indexOf(v)
    }

    @PackageScope
    void normalizeMissingVertices() {
        for( Edge e : edges ) {
            assert e.from || e.to, 'Missing source and termination vertices for edge'

            if( !e.from ) {
                // creates the missing origin vertex
                def vertex = e.from = new Vertex(VertexType.ORIGIN)
                int p = vertices.indexOf( e.to )
                vertices.add( p, vertex )
            }
            else if( !e.to ) {
                // creates the missing termination vertex
                def vertex = e.to = new Vertex(VertexType.TERMINATION)
                int p = vertices.indexOf( e.from )
                vertices.add( p+1, vertex )
            }
        }
    }

    void normalizeEdgeNames(Map map) {
        edges.each { Edge edge ->
            def entry = map.find { k,v -> v.is edge.instance }
            if( entry ) edge.label = entry.key
        }
    }

    String render() {
        normalizeMissingVertices()
        if( session )
            normalizeEdgeNames(session.getBinding().getVariables())
        else
            log.debug "Missing session object -- Cannot normalize edge names"

        def result = []
        result << "digraph graphname {"
        edges.each { edge -> result << edge.render()  }
        result << "}"
        return result.join('\n')
    }


    /**
     *
     * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
     */
    @ToString
    @PackageScope
    class Vertex {

        String label

        VertexType type

        Vertex( VertexType type, String label = null ) {
            this.label = label
            this.type = type
        }

        int getOrder() {
            indexOf(this)
        }

        String getName() { "p${getOrder()}" }

        String render() {

            List attrs = []

            if( isOrigin() || isTermination() ) {
                attrs << "shape=point"
                if( label ) {
                    attrs << "label=\"\""
                    attrs << "xlabel=\"$label\""
                }
            }
            else if( isOperator() ) {
                attrs << "shape=circle"
                attrs << "label=\"\""
                attrs << "fixedsize=true"
                attrs << "width=0.1"
                if( label ) {
                    attrs << "xlabel=\"$label\""
                }
            }
            else if( isProcess() ) {
                if( label )
                    attrs << "label=\"$label\""
            }
            else if( !type ) {
                attrs << "shape=none"
                if( label )
                    attrs << "label=\"$label\""
            }


            return attrs ? "${getName()} [${attrs.join(',')}];" : nul
        }


        boolean isProcess() { type == VertexType.PROCESS }

        boolean isOperator() { type == VertexType.OPERATOR }

        boolean isOrigin() { type == VertexType.ORIGIN }

        boolean isTermination() { type == VertexType.TERMINATION }

    }

    /**
     *
     * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
     */
    @PackageScope
    class Edge {

        // the channel instance
        def instance

        Vertex from

        Vertex to

        String label


        String render() {
            assert from != null || to != null

            String A = from.render()
            String B = to.render()

            def result = new StringBuilder()
            if( A ) result << A << '\n'
            if( B ) result << B << '\n'
            result << "${from.name} -> ${to.name}"
            if( label ) {
                result << " [label=\"${label}\"]"
            }
            result << ";\n"
        }

    }


}