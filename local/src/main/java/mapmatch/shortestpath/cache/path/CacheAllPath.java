package mapmatch.shortestpath.cache.path;

import org.apache.spark.model.st.spatial.graph.RoadGraph;
import org.apache.spark.model.st.spatial.graph.RoadNetwork;
import org.apache.spark.model.st.spatial.graph.RoadNode;
import org.apache.spark.model.st.spatial.graph.RoadSegment;
import org.jgrapht.alg.shortestpath.DijkstraClosestFirstIterator;
import org.jgrapht.alg.util.Pair;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class CacheAllPath {

    public static Map<Tuple2<Integer, Integer>, Double> getMap(RoadNetwork roadNetwork, double radius) {
        return getMap(roadNetwork.getRoadGraph(true), radius);
    }

    public static Map<Tuple2<Integer, Integer>, Double> getMap(RoadGraph roadGraph, double radius) {
        Map<Tuple2<Integer, Integer>, Double> result = new HashMap<>();
        for (RoadNode node : roadGraph.vertexSet()) {
            int startId = node.nodeId();
            final DijkstraClosestFirstIterator iterator = new DijkstraClosestFirstIterator(roadGraph, node, radius);
            while (iterator.hasNext()) {
                iterator.next();
            }
            final Map<RoadNode, Pair<Double, RoadSegment>> distanceMap = iterator.getDistanceAndPredecessorMap();
            distanceMap.forEach((key, value) -> result.put(new Tuple2<>(startId, key.nodeId()), value.getFirst()));
        }
        return result;
    }

    public static Map<Tuple2<RoadNode, RoadNode>, Double> getMapForTest(RoadGraph roadGraph, double radius) {
        Map<Tuple2<RoadNode, RoadNode>, Double> result = new HashMap<>();
        for (RoadNode node : roadGraph.vertexSet()) {
            final DijkstraClosestFirstIterator iterator = new DijkstraClosestFirstIterator(roadGraph, node, radius);
            while (iterator.hasNext()) {
                iterator.next();
            }
            final Map<RoadNode, Pair<Double, RoadSegment>> distanceMap = iterator.getDistanceAndPredecessorMap();
            distanceMap.forEach((key, value) -> result.put(new Tuple2<>(node, key), value.getFirst()));
            break;
        }
        return result;
    }

}
