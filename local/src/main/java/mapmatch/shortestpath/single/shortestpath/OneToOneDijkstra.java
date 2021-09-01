package mapmatch.shortestpath.single.shortestpath;

import org.apache.spark.model.st.spatial.graph.RoadNode;
import org.apache.spark.model.st.spatial.graph.RoadSegment;
import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.DijkstraShortestPath;
import scala.Tuple2;

import java.util.List;

public class OneToOneDijkstra extends AbstractSingleShortestPathAlgo<RoadNode, RoadSegment> {

    private final DijkstraShortestPath<RoadNode, RoadSegment> algo;

    public OneToOneDijkstra(Graph<RoadNode, RoadSegment> graph) {
        super(graph);
        algo = new DijkstraShortestPath<>(graph);
    }

    /**
     * 利用图论，求图中两点的最短路径以及距离
     *
     * @param startNode 起始roadNode
     * @param endNode   终点roadNode
     * @return 最短路径的path 以及距离
     */
    @Override
    public Tuple2<Double, List<RoadSegment>> findShortestPathGraph(RoadNode startNode, RoadNode endNode) {
        GraphPath<RoadNode, RoadSegment> shortestPath = algo.getPath(startNode, endNode);
        return getFinalPath(shortestPath);
    }

}

