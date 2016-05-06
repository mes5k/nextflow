package groovy.runtime.metaclass;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import groovy.lang.MetaClass;
import groovyx.gpars.dataflow.DataflowChannel;
import groovyx.gpars.dataflow.operator.DataflowOperator;
import groovyx.gpars.dataflow.operator.DataflowProcessor;
import nextflow.Global;
import nextflow.ISession;
import nextflow.dag.DAG;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@SuppressWarnings("unchecked")
public class DataflowOperatorDelegatingMetaClass extends groovy.lang.DelegatingMetaClass {

    static final Logger log = LoggerFactory.getLogger(DataflowOperatorDelegatingMetaClass.class);

    private DAG dag;

    public DataflowOperatorDelegatingMetaClass(MetaClass delegate) {
        super(delegate);
        ISession session = Global.getSession();
        if( session == null ) {
            log.warn("Session not defined");
            this.dag = new DAG();
        }
        else {
            this.dag = session.getDag();
        }
    }

    public Object invokeConstructor(Object[] args) {
        DataflowOperator result = (DataflowOperator) delegate.invokeConstructor(args);
        if (args.length == 3 && args[1] instanceof Map) {
            log.trace("Created dataflow operator: {}", result.toString());

            final List<DataflowChannel<?>> inputs = extractInputs((Map) args[1]);
            for( DataflowChannel channel : inputs ) {
                dag.inPort(channel, result);
            }

            final List<DataflowChannel<?>> outputs = extractOutputs((Map) args[1]);
            for( DataflowChannel channel : outputs ) {
                dag.outPort(channel, result);
            }
        }
        else {
            log.trace("Not a valid dataflow operator: {}", result.toString());
        }
        return result;
    }


    static List<DataflowChannel<?>> extractInputs(final Map<String, Object> params) {
        final List<DataflowChannel<?>> inputs = (List<DataflowChannel<?>>) params.get(DataflowProcessor.INPUTS);
        if (inputs == null) return Collections.emptyList();
        return Collections.unmodifiableList(inputs);
    }

    static List<DataflowChannel<?>> extractOutputs(final Map<String, Object> params) {
        final List<DataflowChannel<?>> outputs = (List<DataflowChannel<?>>) params.get(DataflowProcessor.OUTPUTS);
        if (outputs == null) return Collections.emptyList();
        return Collections.unmodifiableList(outputs);
    }

}