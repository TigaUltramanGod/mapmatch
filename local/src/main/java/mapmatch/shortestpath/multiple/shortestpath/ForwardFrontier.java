package mapmatch.shortestpath.multiple.shortestpath;

import org.jgrapht.alg.shortestpath.ContractionHierarchyBidirectionalDijkstra.ContractionSearchFrontier;
import org.jgrapht.alg.shortestpath.ContractionHierarchyPrecomputation.ContractionEdge;
import org.jgrapht.alg.shortestpath.ContractionHierarchyPrecomputation.ContractionVertex;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ForwardFrontier<V, E> {

    protected final V vertex;

    protected ContractionSearchFrontier<ContractionVertex<V>, ContractionEdge<E>> chFrontier;

    protected Map<V, Tuple2<ContractionVertex<V>, Double>> bestTupleMap;

    protected boolean isStop = false;

    public ForwardFrontier(V vertex, ContractionSearchFrontier<ContractionVertex<V>, ContractionEdge<E>> chFrontier, Set<V> sinks) {
        this.vertex = vertex;
        this.chFrontier = chFrontier;
        bestTupleMap = new HashMap<>(sinks.size());
        for (V sink : sinks) {
            bestTupleMap.put(sink, new Tuple2<>(null, Double.POSITIVE_INFINITY));
        }
    }

    // 调用这个方法的时候chFrontier.heap一定还没结束， 且stop一定为false
    public boolean isForwardStop(V otherVertex) {
        final Tuple2<ContractionVertex<V>, Double> tuple = bestTupleMap.get(otherVertex);
        return chFrontier.heap.findMin().getKey() > tuple._2;
    }

}
