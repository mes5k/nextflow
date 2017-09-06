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
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.expression.DataflowExpression
import groovyx.gpars.dataflow.operator.ChoiceClosure
import groovyx.gpars.dataflow.operator.DataflowEventAdapter
import groovyx.gpars.dataflow.operator.DataflowProcessor
import nextflow.Channel
import nextflow.Global
import nextflow.Session
/**
 * Implements the logic for {@link DataflowExtensions#choice} operator(s)
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class ChoiceOp {

    /**
     * The operator source channel "normalised" in a list object.
     * It must contain exactly *one* DataflowReadChannel instance
     */
    private List<DataflowReadChannel> source

    /**
     * The list of output channels resulting from the choice operation
     */
    private List<DataflowWriteChannel> outputs

    /**
     * A closure implementing the *choice* strategy. It returns the index of the
     * selected channel given the actual item emitted by the source channel
     */
    private Closure<Integer> code

    /**
     * {@code true} when the `choice` is applied to a {@link groovyx.gpars.dataflow.DataflowVariable}
     */
    private boolean stopOnFirst

    /**
     * The current nextflow {@link Session} object
     */
    private Session session = (Session)Global.session

    /**
     * Creates the choice operator
     *
     * @param source The source channel either a {@link groovyx.gpars.dataflow.DataflowQueue} or a {@link groovyx.gpars.dataflow.DataflowVariable}
     * @param outputs The list of output channels
     * @param code The closure implementing the *choice* strategy. See {@link #code}
     */
    ChoiceOp(DataflowReadChannel source, List<DataflowWriteChannel> outputs, Closure<Integer> code) {
        assert source
        assert outputs
        this.source = [source]
        this.outputs = outputs
        this.code = code
        this.stopOnFirst = source instanceof DataflowExpression
    }

    /**
     * @return A {@link DataflowEventAdapter} that close properly the output
     * channels when required
     */
    private createListener() {

        def result = new DataflowEventAdapter() {
            @Override
            void afterRun(DataflowProcessor processor, List<Object> messages) {
                if( !stopOnFirst ) return
                // -- terminate the process
                processor.terminate()
                // -- close the output channels
                outputs.each {
                    if( !(it instanceof DataflowExpression))
                        it.bind(Channel.STOP)

                    else if( !(it as DataflowExpression).isBound() )
                        it.bind(Channel.STOP)
                }
            }

            @Override
            public boolean onException(final DataflowProcessor processor, final Throwable e) {
                log.error("@unknown", e)
                session.abort(e)
                return true;
            }
        }

        return [result]
    }

    /**
     * Applies the choice operator
     */
    def apply() {

        def params = [
                inputs: source,
                outputs: outputs,
                listeners: createListener()
        ]

        DataflowHelper.newOperator(params, new ChoiceClosure(code))
    }


}
