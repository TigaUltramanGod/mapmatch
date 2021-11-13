package mapmatch.shortestpath.single.shortestpath;

import org.apache.spark.model.st.spatial.graph.RoadNode;
import org.apache.spark.model.st.spatial.graph.RoadSegment;
import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.BidirectionalAStarShortestPath;
import point.GeoFunction;
import scala.Tuple2;

import java.util.List;

public class OneToOneBiAstar extends AbstractSingleShortestPathAlgo<RoadNode, RoadSegment> {

    private final BidirectionalAStarShortestPath<RoadNode, RoadSegment> algo;

    public OneToOneBiAstar(Graph<RoadNode, RoadSegment> graph) {
        super(graph);

        algo = new BidirectionalAStarShortestPath<>(graph, GeoFunction::getDistanceInM);
    }

    @Override
    public Tuple2<Double, List<RoadSegment>> findShortestPathGraph(RoadNode startNode, RoadNode endNode) {
        GraphPath<RoadNode, RoadSegment> shortestPath = algo.getPath(startNode, endNode);
        return getFinalPath(shortestPath);
    }
}
