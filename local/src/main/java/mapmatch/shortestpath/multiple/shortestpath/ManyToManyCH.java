package mapmatch.shortestpath.multiple.shortestpath;

import mapmatch.shortestpath.single.shortestpath.OneToOneCH;
import org.apache.spark.model.st.spatial.graph.RoadNode;
import org.apache.spark.model.st.spatial.graph.RoadSegment;
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

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ManyToManyCH extends AbstractMultipleShortestPathAlgo<RoadNode, RoadSegment> {

    private final ContractionHierarchy<RoadNode, RoadSegment> ch;

    private Map<RoadNode, ForwardFrontier<RoadNode, RoadSegment>> forwardFrontiers;


    private final Supplier<AddressableHeap<Double, Pair<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>>>> heapSupplier;

    private Map<RoadNode, ContractionSearchFrontier<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>>> backwardFrontiers;

    public ManyToManyCH(ContractionHierarchy<RoadNode, RoadSegment> hierarchy) {
        super(hierarchy.getGraph());
        ch = hierarchy;
        heapSupplier = PairingHeap::new;
    }

    @Override
    public Map<Tuple2<RoadNode, RoadNode>, Double> findAllPath(Set<RoadNode> startNodes, Set<RoadNode> endNodes) {
        getAllPaths(startNodes, endNodes);
        Map<Tuple2<RoadNode, RoadNode>, Double> pathMap = new HashMap<>();
        for (RoadNode source : startNodes) {
            for (RoadNode target : endNodes) {
                final Tuple2<RoadNode, RoadNode> key = new Tuple2<>(source, target);
                if (source.equals(target)) {
                    pathMap.put(key, 0d);
                } else {
                    final ForwardFrontier<RoadNode, RoadSegment> frontier = forwardFrontiers.get(source);
                    double bestPath = frontier.bestTupleMap.get(target)._2;
                    pathMap.put(key, bestPath);
                }
            }
        }
        return pathMap;
    }

    @Override
    public Tuple2<Double, List<RoadSegment>> findShortestPathGraph(RoadNode startNode, RoadNode endNode) {
        return new OneToOneCH(ch).findShortestPathGraph(startNode, endNode);
    }

    private void getAllPaths(Set<RoadNode> sources, Set<RoadNode> sinks) {
        forwardFrontiers = new HashMap<>(sources.size());
        backwardFrontiers = new HashMap<>(sinks.size());
        final MaskSubgraph<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>> forwardGraph =
                new MaskSubgraph<>(ch.getContractionGraph(), v -> false, e -> !e.isUpward);
        final MaskSubgraph<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>> backwardGraph =
                new MaskSubgraph<>(new EdgeReversedGraph<>(ch.getContractionGraph()), v -> false, e -> e.isUpward);
        for (RoadNode source : sources) {
            ContractionSearchFrontier<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>> forwardFrontier =
                    new ContractionSearchFrontier<>(forwardGraph, heapSupplier);
            forwardFrontier.updateDistance(ch.getContractionMapping().get(source), null, 0d);
            forwardFrontiers.put(source, new ForwardFrontier<>(source, forwardFrontier, sinks));
        }
        for (RoadNode sink : sinks) {
            ContractionSearchFrontier<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>> backwardFrontier =
                    new ContractionSearchFrontier<>(backwardGraph, heapSupplier);
            backwardFrontier.updateDistance(ch.getContractionMapping().get(sink), null, 0d);
            backwardFrontiers.put(sink, backwardFrontier);
        }
        while (!forwardFrontiers.values().stream().allMatch(i -> i.isStop) || !backwardFrontiers.values().stream().allMatch(i -> i.isFinished)) {
            forwardMove();
            backwardMove();
        }
    }

    private void forwardMove() {
        for (ForwardFrontier<RoadNode, RoadSegment> forwardFrontier : forwardFrontiers.values()) {
            if (forwardFrontier.isStop) {
                continue;
            }
            if (forwardFrontier.chFrontier.heap.isEmpty() ||
                    backwardFrontiers.keySet().stream().allMatch(forwardFrontier::isForwardStop)) {
                forwardFrontier.isStop = true;
            } else {
                AddressableHeap.Handle<Double,
                        Pair<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>>> node =
                        forwardFrontier.chFrontier.heap.deleteMin();
                ContractionVertex<RoadNode> v = node.getValue().getFirst();
                double vDistance = node.getKey();

                for (ContractionEdge<RoadSegment> e : forwardFrontier.chFrontier.graph.outgoingEdgesOf(v)) {
                    ContractionVertex<RoadNode> u = forwardFrontier.chFrontier.graph.getEdgeTarget(e);
                    double eWeight = forwardFrontier.chFrontier.graph.getEdgeWeight(e);
                    double newDist = vDistance + eWeight;
                    forwardFrontier.chFrontier.updateDistance(u, e, newDist);
                    for (Map.Entry<RoadNode, ContractionSearchFrontier<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>>> entry : backwardFrontiers.entrySet()) {
                        RoadNode sink = entry.getKey();
                        final Tuple2<ContractionVertex<RoadNode>, Double> tuple2 = forwardFrontier.bestTupleMap.get(sink);
                        double bestDist = tuple2._2;
                        double pathDistance = newDist + entry.getValue().getDistance(u);
                        if (pathDistance < bestDist) {
                            forwardFrontier.bestTupleMap.put(sink, new Tuple2<>(u, pathDistance));
                        }
                    }
                }
            }
        }
    }

    private void backwardMove() {
        for (Map.Entry<RoadNode, ContractionSearchFrontier<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>>> entry : backwardFrontiers.entrySet()) {
            RoadNode sink = entry.getKey();
            ContractionSearchFrontier<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>> backwardFrontier = entry.getValue();
            if (backwardFrontier.isFinished) {
                continue;
            }
            if (backwardFrontier.heap.isEmpty() ||
                    backwardFrontier.heap.findMin().getKey() > Collections.max(
                            forwardFrontiers.values().stream().map(i -> i.bestTupleMap.get(sink)._2).collect(Collectors.toList())
                    )) {
                backwardFrontier.isFinished = true;
            } else {
                AddressableHeap.Handle<Double,
                        Pair<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>>> node =
                        backwardFrontier.heap.deleteMin();
                ContractionVertex<RoadNode> v = node.getValue().getFirst();
                double vDistance = node.getKey();

                for (ContractionEdge<RoadSegment> e : backwardFrontier.graph.outgoingEdgesOf(v)) {
                    ContractionVertex<RoadNode> u = backwardFrontier.graph.getEdgeTarget(e);
                    double eWeight = backwardFrontier.graph.getEdgeWeight(e);
                    double newDist = vDistance + eWeight;
                    backwardFrontier.updateDistance(u, e, newDist);
                    for (ForwardFrontier<RoadNode, RoadSegment> forwardFrontier : forwardFrontiers.values()) {
                        final Tuple2<ContractionVertex<RoadNode>, Double> tuple2 = forwardFrontier.bestTupleMap.get(sink);
                        double bestDist = tuple2._2;
                        double pathDistance = newDist + forwardFrontier.chFrontier.getDistance(u);
                        if (pathDistance < bestDist) {
                            forwardFrontier.bestTupleMap.put(sink, new Tuple2<>(u, pathDistance));
                        }
                    }
                }
            }
        }
    }

}
