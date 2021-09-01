package mapmatch.shortestpath.single.shortestpath;

import org.apache.spark.model.st.spatial.graph.RoadNode;
import org.apache.spark.model.st.spatial.graph.RoadSegment;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.ContractionHierarchyBidirectionalDijkstra.ContractionSearchFrontier;
import org.jgrapht.alg.shortestpath.ContractionHierarchyPrecomputation.ContractionEdge;
import org.jgrapht.alg.shortestpath.ContractionHierarchyPrecomputation.ContractionHierarchy;
import org.jgrapht.alg.shortestpath.ContractionHierarchyPrecomputation.ContractionVertex;
import org.jgrapht.alg.util.Pair;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.graph.MaskSubgraph;
import org.jheaps.AddressableHeap;
import org.jheaps.tree.PairingHeap;
import scala.Tuple2;

import java.util.List;
import java.util.function.Supplier;

public class OneToOneCH extends AbstractSingleShortestPathAlgo<RoadNode, RoadSegment> {

    private final ContractionHierarchy<RoadNode, RoadSegment> ch;

    private final Supplier<
            AddressableHeap<Double, Pair<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>>>> heapSupplier;

    private ContractionSearchFrontier<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>> forwardFrontier;

    private ContractionSearchFrontier<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>> backwardFrontier;

    private ContractionVertex<RoadNode> bestPathCommonVertex = null;

    private double bestPath = Double.POSITIVE_INFINITY;

    public OneToOneCH(ContractionHierarchy<RoadNode, RoadSegment> hierarchy) {
        super(hierarchy.getGraph());
        this.ch = hierarchy;
        this.heapSupplier = PairingHeap::new;
    }

    @Override
    public Tuple2<Double, List<RoadSegment>> findShortestPathGraph(RoadNode startNode, RoadNode endNode) {
        return getFinalPath(getPath(startNode, endNode));
    }

    private void reInitial() {
        bestPathCommonVertex = null;
        bestPath = Double.POSITIVE_INFINITY;
    }

    /**
     * {@inheritDoc}
     */
    private GraphPath<RoadNode, RoadSegment> getPath(RoadNode source, RoadNode sink) {
        if (source.equals(sink)) {
            return createEmptyPath(source, sink);
        }
        ContractionVertex<RoadNode> contractedSource = ch.getContractionMapping().get(source);
        ContractionVertex<RoadNode> contractedSink = ch.getContractionMapping().get(sink);
        // create frontiers
        forwardFrontier = new ContractionSearchFrontier<>(new MaskSubgraph<>(ch.getContractionGraph(), v -> false, e -> !e.isUpward), heapSupplier);
        backwardFrontier = new ContractionSearchFrontier<>(new MaskSubgraph<>(new EdgeReversedGraph<>(ch.getContractionGraph()), v -> false, e -> e.isUpward), heapSupplier);
        // initialize both frontiers
        forwardFrontier.updateDistance(contractedSource, null, 0d);
        backwardFrontier.updateDistance(contractedSink, null, 0d);
        while (!forwardFrontier.isFinished || !backwardFrontier.isFinished) {
            forwardMove();
            backwardMove();
        }
        GraphPath<RoadNode, RoadSegment> path;
        if (Double.isFinite(bestPath)) {
            path = createPath(forwardFrontier, backwardFrontier, bestPath, contractedSource, bestPathCommonVertex, contractedSink, ch);
        } else {
            path = createEmptyPath(source, sink);
        }
        reInitial();
        return path;
    }

    private void forwardMove() {
        if (forwardFrontier.heap.isEmpty() || forwardFrontier.heap.findMin().getKey() > bestPath) {
            forwardFrontier.isFinished = true;
        }
        if (forwardFrontier.isFinished) {
            return;
        }
        AddressableHeap.Handle<Double, Pair<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>>> node = forwardFrontier.heap.deleteMin();
        ContractionVertex<RoadNode> v = node.getValue().getFirst();
        Double vDistance = node.getKey();
        for (ContractionEdge<RoadSegment> e : forwardFrontier.graph.outgoingEdgesOf(v)) {
            ContractionVertex<RoadNode> u = forwardFrontier.graph.getEdgeTarget(e);
            double eWeight = forwardFrontier.graph.getEdgeWeight(e);
            forwardFrontier.updateDistance(u, e, vDistance + eWeight);
            double newDist = vDistance + eWeight + backwardFrontier.getDistance(u);
            if (newDist < bestPath) {
                bestPathCommonVertex = u;
                bestPath = newDist;
            }
        }
    }

    private void backwardMove() {
        if (backwardFrontier.heap.isEmpty() || backwardFrontier.heap.findMin().getKey() > bestPath) {
            backwardFrontier.isFinished = true;
        }
        if (backwardFrontier.isFinished) {
            return;
        }
        AddressableHeap.Handle<Double, Pair<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>>> node = backwardFrontier.heap.deleteMin();
        ContractionVertex<RoadNode> v = node.getValue().getFirst();
        double vDistance = node.getKey();
        for (ContractionEdge<RoadSegment> e : backwardFrontier.graph.outgoingEdgesOf(v)) {
            ContractionVertex<RoadNode> u = backwardFrontier.graph.getEdgeTarget(e);
            double eWeight = backwardFrontier.graph.getEdgeWeight(e);
            backwardFrontier.updateDistance(u, e, vDistance + eWeight);
            double newDist = vDistance + eWeight + forwardFrontier.getDistance(u);
            if (newDist < bestPath) {
                bestPath = newDist;
                bestPathCommonVertex = u;
            }
        }
    }

}

