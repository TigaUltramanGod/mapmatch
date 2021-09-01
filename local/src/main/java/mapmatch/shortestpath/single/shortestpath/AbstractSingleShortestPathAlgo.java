package mapmatch.shortestpath.single.shortestpath;

import mapmatch.shortestpath.*;
import org.apache.spark.model.st.spatial.graph.RoadNetwork;
import org.jgrapht.Graph;

import java.util.Objects;

/**
 * 最短路径算法的抽象类
 *
 * @date 2020-12-24 17:49
 */
public abstract class AbstractSingleShortestPathAlgo<V, E> extends AbstractShortestPathAlgo {

    public AbstractSingleShortestPathAlgo(Graph<V, E> graph) {
        super(graph);
    }

    public static AbstractSingleShortestPathAlgo getAlgo(RoadNetwork roadNetwork, ShortestPathAlgoTypeEnum algoType) {
        switch (algoType) {
            case ASTAR:
                return new OneToOneAStar(roadNetwork.getRoadGraph(true));
            case DIJKSTRA:
                return new OneToOneDijkstra(roadNetwork.getRoadGraph(true));
            case BI_DIJKSTRA:
                return new OneToOneBiDijkstra(roadNetwork.getRoadGraph(true));
            case CH:
                Objects.requireNonNull(roadNetwork.getCHGraph(), "contraction graph should not be null!");
                return new OneToOneCH(roadNetwork.getCHGraph());
        }
        return new OneToOneAStar(roadNetwork.getRoadGraph(true));
    }

}
