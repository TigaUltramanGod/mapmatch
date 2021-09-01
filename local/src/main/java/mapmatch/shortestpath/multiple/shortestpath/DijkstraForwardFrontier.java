package mapmatch.shortestpath.multiple.shortestpath;

import org.jgrapht.alg.shortestpath.BidirectionalDijkstraShortestPath.*;
import org.jgrapht.alg.shortestpath.ContractionHierarchyPrecomputation;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DijkstraForwardFrontier<V,E> {

    protected final V vertex;

    protected DijkstraSearchFrontier<V, E> frontier;

    protected Map<V, Tuple2<V, Double>> bestTupleMap;

    protected boolean isStop = false;

    public DijkstraForwardFrontier(V vertex, DijkstraSearchFrontier<V, E> frontier, Set<V> sinks) {
        this.vertex = vertex;
        this.frontier = frontier;
        bestTupleMap = new HashMap<>(sinks.size());
        for (V sink : sinks) {
            bestTupleMap.put(sink, new Tuple2<>(null, Double.POSITIVE_INFINITY));
        }
    }

    public boolean isForwardStop(V otherVertex, DijkstraSearchFrontier<V,E> otherFrontier) {
        if (otherFrontier.heap.isEmpty()) {
            return true;
        }
        final Tuple2<V, Double> tuple = bestTupleMap.get(otherVertex);
        return frontier.heap.findMin().getKey() + otherFrontier.heap.findMin().getKey() > tuple._2;
    }
}
