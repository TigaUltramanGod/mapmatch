package mapmatch.shortestpath.multiple.shortestpath;

import mapmatch.shortestpath.single.shortestpath.OneToOneDijkstra;
import org.apache.spark.model.st.spatial.graph.RoadNode;
import org.apache.spark.model.st.spatial.graph.RoadSegment;
import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.alg.shortestpath.BidirectionalDijkstraShortestPath.*;
import org.jgrapht.alg.util.Pair;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jheaps.AddressableHeap;
import org.jheaps.tree.PairingHeap;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class ManyToManyBiDijkstra extends AbstractMultipleShortestPathAlgo<RoadNode, RoadSegment> {

    private final Supplier<AddressableHeap<Double, Pair<RoadNode, RoadSegment>>> heapSupplier;

    private Map<RoadNode, DijkstraForwardFrontier<RoadNode, RoadSegment>> forwardFrontiers;

    private Map<RoadNode, DijkstraSearchFrontier<RoadNode, RoadSegment>> backwardFrontiers;

    public ManyToManyBiDijkstra(Graph<RoadNode, RoadSegment> graph) {
        super(graph);
        this.heapSupplier = PairingHeap::new;
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
                    final DijkstraForwardFrontier<RoadNode, RoadSegment> frontier = forwardFrontiers.get(source);
                    final double bestPath = frontier.bestTupleMap.get(target)._2;
                    pathMap.put(key, bestPath);
                }
            }
        }
        return pathMap;
    }

    @Override
    public Tuple2<Double, List<RoadSegment>> findShortestPathGraph(RoadNode startNode, RoadNode endNode) {
        return new OneToOneDijkstra(graph).findShortestPathGraph(startNode, endNode);
    }

    public void getAllPaths(Set<RoadNode> sources, Set<RoadNode> sinks) {
        forwardFrontiers = new HashMap<>(sources.size());
        backwardFrontiers = new HashMap<>(sinks.size());
        final EdgeReversedGraph<RoadNode, RoadSegment> reversedGraph = new EdgeReversedGraph<RoadNode, RoadSegment>(graph);

        for (RoadNode source : sources) {
            DijkstraSearchFrontier<RoadNode, RoadSegment> forwardFrontier =
                    new DijkstraSearchFrontier<RoadNode, RoadSegment>(graph, heapSupplier);
            forwardFrontier.updateDistance(source, null, 0d);
            forwardFrontiers.put(source, new DijkstraForwardFrontier<>(source, forwardFrontier, sinks));
        }
        for (RoadNode sink : sinks) {
            DijkstraSearchFrontier<RoadNode, RoadSegment> backwardFrontier =
                    new DijkstraSearchFrontier<>(reversedGraph, heapSupplier);
            backwardFrontier.updateDistance(sink, null, 0d);
            backwardFrontiers.put(sink, backwardFrontier);
        }
        while (!forwardFrontiers.values().stream().allMatch(i -> i.isStop) || !backwardFrontiers.values().stream().allMatch(i -> i.isFinished)) {
            forwardMove();
            backwardMove();
        }
    }

    private GraphPath<RoadNode, RoadSegment> getPath(RoadNode source, RoadNode sink) {
        if (source.equals(sink)) {
            return createEmptyPath(source, sink);
        }
        final DijkstraForwardFrontier<RoadNode, RoadSegment> frontier = forwardFrontiers.get(source);
        final Tuple2<RoadNode, Double> tuple = frontier.bestTupleMap.get(sink);
        double bestPath = tuple._2;
        final DijkstraSearchFrontier<RoadNode, RoadSegment> forwardFrontier = frontier.frontier;
        final DijkstraSearchFrontier<RoadNode, RoadSegment> backwardFrontier = backwardFrontiers.get(sink);
        // create path if found
        double radius = Double.POSITIVE_INFINITY;
        if (Double.isFinite(bestPath) && bestPath <= radius) {
            return createPath(forwardFrontier, backwardFrontier, bestPath, source, tuple._1, sink);
        } else {
            return createEmptyPath(source, sink);
        }
    }

    private void forwardMove() {
        for (DijkstraForwardFrontier<RoadNode, RoadSegment> forwardFrontier : forwardFrontiers.values()) {
            if (forwardFrontier.isStop) {
                continue;
            }
            if (forwardFrontier.frontier.heap.isEmpty() ||
                    backwardFrontiers.entrySet().stream().allMatch(entry -> forwardFrontier.isForwardStop(entry.getKey(), entry.getValue()))) {
                forwardFrontier.isStop = true;
            } else {

                AddressableHeap.Handle<Double, Pair<RoadNode, RoadSegment>> node = forwardFrontier.frontier.heap.deleteMin();
                RoadNode v = node.getValue().getFirst();
                double vDistance = node.getKey();

                for (RoadSegment e : forwardFrontier.frontier.graph.outgoingEdgesOf(v)) {
                    RoadNode u = Graphs.getOppositeVertex(forwardFrontier.frontier.graph, e, v);
                    double eWeight = forwardFrontier.frontier.graph.getEdgeWeight(e);
                    forwardFrontier.frontier.updateDistance(u, e, vDistance + eWeight);
                    for (Map.Entry<RoadNode, DijkstraSearchFrontier<RoadNode, RoadSegment>> entry : backwardFrontiers.entrySet()) {
                        RoadNode sink = entry.getKey();
                        final Tuple2<RoadNode, Double> tuple2 = forwardFrontier.bestTupleMap.get(sink);
                        double bestDist = tuple2._2();
                        double pathDistance = vDistance + eWeight + entry.getValue().getDistance(u);
                        if (pathDistance < bestDist) {
                            forwardFrontier.bestTupleMap.put(sink, new Tuple2<>(u, pathDistance));
                        }
                    }
                }
            }
        }
    }

    private void backwardMove() {
        for (Map.Entry<RoadNode, DijkstraSearchFrontier<RoadNode, RoadSegment>> entry : backwardFrontiers.entrySet()) {
            RoadNode sink = entry.getKey();
            DijkstraSearchFrontier<RoadNode, RoadSegment> backwardFrontier = entry.getValue();
            if (backwardFrontier.isFinished) {
                continue;
            }
            if (backwardFrontier.heap.isEmpty() ||
                    forwardFrontiers.values().stream().allMatch(frontier -> {
                        if (frontier.isStop || frontier.frontier.heap.isEmpty()) {
                            return true;
                        }
                        final Double bestDist = frontier.bestTupleMap.get(sink)._2;
                        return backwardFrontier.heap.findMin().getKey() + frontier.frontier.heap.findMin().getKey() > bestDist;
                    })) {
                backwardFrontier.isFinished = true;
            } else {

                AddressableHeap.Handle<Double, Pair<RoadNode, RoadSegment>> node = backwardFrontier.heap.deleteMin();
                RoadNode v = node.getValue().getFirst();
                double vDistance = node.getKey();

                for (RoadSegment e : backwardFrontier.graph.outgoingEdgesOf(v)) {
                    RoadNode u = Graphs.getOppositeVertex(backwardFrontier.graph, e, v);
                    double eWeight = backwardFrontier.graph.getEdgeWeight(e);
                    double newDist = vDistance + eWeight;
                    backwardFrontier.updateDistance(u, e, newDist);
                    for (DijkstraForwardFrontier<RoadNode, RoadSegment> forwardFrontier : forwardFrontiers.values()) {
                        final Tuple2<RoadNode, Double> tuple2 = forwardFrontier.bestTupleMap.get(sink);
                        double bestDist = tuple2._2;
                        double pathDist = newDist + forwardFrontier.frontier.getDistance(u);
                        if (pathDist < bestDist) {
                            forwardFrontier.bestTupleMap.put(sink, new Tuple2<>(u, pathDist));
                        }
                    }
                }
            }
        }
    }

}
