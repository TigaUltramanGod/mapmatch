package mapmatch.shortestpath.multiple.shortestpath;

import mapmatch.shortestpath.AbstractShortestPathAlgo;
import mapmatch.shortestpath.ShortestPathAlgoTypeEnum;
import org.apache.spark.model.st.spatial.graph.RoadNetwork;
import org.apache.spark.model.st.spatial.graph.RoadNode;
import org.jgrapht.Graph;
import point.CandidatePoint;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractMultipleShortestPathAlgo<V, E> extends AbstractShortestPathAlgo {

    public AbstractMultipleShortestPathAlgo(Graph<V, E> graph) {
        super(graph);
    }

    public static AbstractMultipleShortestPathAlgo getAlgo(RoadNetwork roadNetwork, ShortestPathAlgoTypeEnum algoType) throws Exception {
        switch (algoType) {
            case BI_DIJKSTRA:
                return new ManyToManyBiDijkstra(roadNetwork.getRoadGraph(true));
            case CH:
                Objects.requireNonNull(roadNetwork.getCHGraph(), "contraction graph should not be null!");
                return new ManyToManyCH(roadNetwork.getCHGraph());
            default:
                throw new Exception("unsupported shortestPath algorithm");
        }
    }

    public Map<Tuple2<RoadNode,RoadNode>, Double> findAllPath(List<CandidatePoint> startCandidates, List<CandidatePoint> endCandidates, RoadNetwork roadNetwork) {
        Set<RoadNode> startNodes = startCandidates.stream().map(i -> roadNetwork.getRoadSegment(i.getRoadSegmentID()).getEndNode()).collect(Collectors.toSet());
        Set<RoadNode> endNodes = endCandidates.stream().map(i -> roadNetwork.getRoadSegment(i.getRoadSegmentID()).getStartNode()).collect(Collectors.toSet());
        return findAllPath(startNodes, endNodes);
    }

    public abstract Map<Tuple2<RoadNode,RoadNode>, Double> findAllPath(Set<RoadNode> startNodes, Set<RoadNode> endNodes);


}
