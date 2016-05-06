package nextflow.dag

import groovy.transform.CompileStatic

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class DAG {

    List<Edge> edges = new ArrayList<>(50);

    public void inPort( def entering, def operator )  {
        def edge = find(entering)
        if( !edge ) {
            edges << new Edge(instance: entering, to: new Vertex(operator))
        }
        else if( edge.to == null ) {
            edge.to = new Vertex(operator)
        }
        else {
            throw new DuplicateInputEdgeException()
        }
    }

    public void outPort( def leaving, def operator ) {

        def edge = find(leaving)
        if( !edge ) {
            edges << new Edge(instance: leaving, from: new Vertex(operator))
        }
        else if( edge.from == null ) {
            edge.from = new Vertex(operator)
        }
        else {
            throw new DuplicateOutputEdgeException()
        }

    }

    Edge find( def channel ) {
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