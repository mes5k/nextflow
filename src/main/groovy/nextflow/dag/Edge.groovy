package nextflow.dag
import groovy.transform.CompileStatic
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class Edge {

    // the channel instance
    def instance

    Vertex from

    Vertex to

    String label


    String toString() {
        assert from != null || to != null

        def result = new StringBuilder()
        def A = renderVertex(from, result)
        def B = renderVertex(to, result)
        result << "$A -> $B;"

    }

    private String renderVertex( Vertex vertex, StringBuilder result ) {
        def label
        if( !vertex ) {
            label = "_${Integer.toHexString(instance.hashCode())}"
            result << "${label} [shape=point];\n"
        }
        else {
            label = "p${vertex.index}"
        }
        return label
    }


}
