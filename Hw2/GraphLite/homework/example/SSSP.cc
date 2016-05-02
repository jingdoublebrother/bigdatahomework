/* 0, 2015E8015061092, Zeng Jing */
/**
 * @file SSSP.cc
 * @author liqing(liqingdoublebrother@gmail.com)
 * @version 0.1
 *
 * This file implements the SSSP(Single Source Shortest Path) algorithm using GraphLite API.
 */

#include <stdio.h>
#include <string.h>
#include <math.h>

#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) SSSP##name

#define VERTEXVALUETYPE int
#define INFCOST 0x0fffffff

// The source vertex id
int64_t v0_id;

class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {
public:
    int64_t getVertexNum() {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex= n;
        return m_total_vertex;
    }
    int64_t getEdgeNum() {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge= n;
        return m_total_edge;
    }
    int getVertexValueSize() {
        m_n_value_size = sizeof(VERTEXVALUETYPE);
        return m_n_value_size;
    }
    int getEdgeValueSize() {
        m_e_value_size = sizeof(VERTEXVALUETYPE);
        return m_e_value_size;
    }
    int getMessageValueSize() {
        m_m_value_size = sizeof(VERTEXVALUETYPE);
        return m_m_value_size;
    }
    void loadGraph() {
        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        VERTEXVALUETYPE weight;

        // Initially, every vertex value is infinite
        VERTEXVALUETYPE initial_value = INFCOST;

        int outdegree = 0;

        const char *line= getEdgeLine();

        // Note: modify this if an edge weight is to be read
        //       modify the 'weight' variable

        sscanf(line, "%lld %lld %d", &from, &to, &weight);
        addEdge(from, to, &weight);

        last_vertex = from;
        ++outdegree;
        for (int64_t i = 1; i < m_total_edge; ++i) {
            line= getEdgeLine();

            // Note: modify this if an edge weight is to be read
            //       modify the 'weight' variable
            sscanf(line, "%lld %lld %d", &from, &to, &weight);

            if (last_vertex != from) {
                addVertex(last_vertex, &initial_value, outdegree);
                last_vertex = from;
                outdegree = 1;
            } else {
                ++outdegree;
            }
            addEdge(from, to, &weight);
        }
        addVertex(last_vertex, &initial_value, outdegree);
    }
};

class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter {
public:
    void writeResult() {
        int64_t vid;
        VERTEXVALUETYPE value;
        char s[1024];

        for (ResultIterator r_iter; ! r_iter.done(); r_iter.next() ) {
            r_iter.getIdValue(vid, &value);
            int n = sprintf(s, "%lld: %d\n", (unsigned long long)vid, value);
            writeNextResLine(s, n);
        }
    }
};

// An aggregator that records a VERTEXVALUETYPE value tom compute min
class VERTEX_CLASS_NAME(Aggregator): public Aggregator<VERTEXVALUETYPE> {
public:
    void init() {
        m_global = 0;
        m_local = 0;
    }
    void* getGlobal() {
        return &m_global;
    }
    void setGlobal(const void* p) {
        m_global = * (VERTEXVALUETYPE *)p;
    }
    void* getLocal() {
        return &m_local;
    }
    void merge(const void* p) {
        m_global = (m_global < *(VERTEXVALUETYPE *)p) ? m_global : *(VERTEXVALUETYPE *)p;
    }
    void accumulate(const void* p) {
        m_local = (m_local < * (VERTEXVALUETYPE *)p) ? m_local : * (VERTEXVALUETYPE *)p;
    }
};

class VERTEX_CLASS_NAME(): public Vertex <VERTEXVALUETYPE, VERTEXVALUETYPE, VERTEXVALUETYPE> {
public:
    void compute(MessageIterator* pmsgs) {
        // Only on the first superstep( superstep 0 ), the source vertex value turns to be 0 from INFCOST.
        VERTEXVALUETYPE min_val = (v0_id == getVertexId()) ? 0 : INFCOST;

        for ( ; ! pmsgs->done(); pmsgs->next() )
            min_val = min_val < pmsgs->getValue() ? min_val : pmsgs->getValue();

        // Only when the shortest path value is changed, updates the value and sends messages.
        if( min_val < getValue() ){
            * mutableValue() = min_val;
            OutEdgeIterator oei = getOutEdgeIterator();
            for( ; !oei.done(); oei.next() )
                sendMessageTo( oei.target(), min_val + oei.getValue() );
            accumulateAggr(0, &min_val);
        }
    // Halt and to be inactive statue, to be active again when recives messages.
    // End all computing work when all vertexs are inactive.
    voteToHalt();
    }
};

class VERTEX_CLASS_NAME(Graph): public Graph {
public:
    VERTEX_CLASS_NAME(Aggregator)* aggregator;

public:
    // argv[0]: SSSP.so
    // argv[1]: <input path>
    // argv[2]: <output path>
    // argv[3]: v0_id
    void init(int argc, char* argv[]) {

        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if (argc < 4) {
           printf ("Usage: %s <input path> <output path> <v0_id>\n", argv[0]);
           exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];
        v0_id = strtoul( argv[3], NULL, 10 );

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];
        regNumAggr(1);
        regAggr(0, &aggregator[0]);
    }

    void term() {
        delete[] aggregator;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph* create_graph() {
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) {
    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);
    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);
    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);
    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;
}
