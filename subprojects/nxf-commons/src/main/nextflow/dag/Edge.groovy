package nextflow.dag
import groovy.transform.CompileStatic
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class Edge {

    // the channel instance
    def instance;

    Vertex from;

    Vertex to;


    String toString() {
        assert from != null || to != null

        def result = new StringBuilder()
        def A = renderNode(from, result)
        def B = renderNode(to, result)
        result << "$A -> $B;"

    }

    private String renderNode( Vertex vertex, StringBuilder result ) {
        def label
        if( !vertex ) {
            label = "_${Integer.toHexString(instance.hashCode())}"
            result << "${label} [shape=point];\n"
        }
        else {
            label = "_${Integer.toHexString(vertex.instance.hashCode())}"
        }
        return label
    }


}
