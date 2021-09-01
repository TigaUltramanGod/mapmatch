package mapmatch.shortestpath;

import org.apache.spark.model.st.spatial.graph.RoadNetwork;
import org.apache.spark.model.st.spatial.graph.RoadNode;
import org.apache.spark.model.st.spatial.graph.RoadSegment;
import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.alg.shortestpath.BaseBidirectionalShortestPathAlgorithm.*;
import org.jgrapht.alg.shortestpath.ContractionHierarchyBidirectionalDijkstra.*;
import org.jgrapht.alg.shortestpath.ContractionHierarchyPrecomputation.*;
import org.jgrapht.graph.GraphWalk;
import point.CandidatePoint;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public abstract class AbstractShortestPathAlgo<V,E> {

    protected Graph<V, E> graph;

    public AbstractShortestPathAlgo(Graph<V, E> graph) {
        this.graph = graph;
    }

    public abstract Tuple2<Double, List<RoadSegment>> findShortestPathGraph(RoadNode startNode, RoadNode endNode);

    public Tuple2<Double, List<RoadSegment>> findShortestPathByCandidatePoint(CandidatePoint preCandiPt, CandidatePoint curCandiPt, RoadNetwork roadNetwork) {
        RoadNode preV = roadNetwork.getRoadSegment(preCandiPt.getRoadSegmentID()).getEndNode();
        RoadNode curU = roadNetwork.getRoadSegment(curCandiPt.getRoadSegmentID()).getStartNode();
        return shortestPathInDifferentRoad(findShortestPathGraph(preV, curU), roadNetwork, preCandiPt, curCandiPt);
    }

    public Tuple2<Double, List<RoadSegment>> shortestPathInDifferentRoad(Tuple2<Double, List<RoadSegment>> shortestPathGraph, RoadNetwork roadNetwork, CandidatePoint preCandiPt, CandidatePoint curCandiPt) {
        try {
            RoadSegment preRs = roadNetwork.getRoadSegment(preCandiPt.getRoadSegmentID());
            double distToSrc = preRs.getLength() - preCandiPt.getOffset();
            double distToEnd = curCandiPt.getOffset();
            List<RoadSegment> path = shortestPathGraph._2;
            Double dist = shortestPathGraph._1 + distToSrc + distToEnd;
            return new Tuple2<>(dist, path);
        } catch (Exception e) {
            return new Tuple2<>(Double.MAX_VALUE, null);
        }
    }

    protected Tuple2<Double, List<RoadSegment>> getFinalPath(GraphPath<RoadNode, RoadSegment> graphPath) {
        double dist = 0.0;
        List<RoadSegment> path = new ArrayList<>();
        if (null == graphPath) {
            return new Tuple2<>(Double.MAX_VALUE, null);
        }
        for (RoadSegment rs : graphPath.getEdgeList()) {
            dist += rs.getLength();
            path.add(rs);
        }
        return new Tuple2<>(dist, path);
    }

    protected final GraphPath<V, E> createEmptyPath(V source, V sink) {
        if (source.equals(sink)) {
            return GraphWalk.singletonWalk(graph, source, 0d);
        } else {
            return null;
        }
    }

    protected GraphPath<V, E> createPath(
            ContractionSearchFrontier<ContractionVertex<V>, ContractionEdge<E>> forwardFrontier,
            ContractionSearchFrontier<ContractionVertex<V>, ContractionEdge<E>> backwardFrontier,
            double weight, ContractionVertex<V> source, ContractionVertex<V> commonVertex,
            ContractionVertex<V> sink, ContractionHierarchy<V, E> hierarchy) {

        LinkedList<E> edgeList = new LinkedList<>();
        LinkedList<V> vertexList = new LinkedList<>();

        // add common vertex
        vertexList.add(commonVertex.vertex);
        // traverse forward path
        ContractionVertex<V> v = commonVertex;
        while (true) {
            ContractionEdge<E> e = forwardFrontier.getTreeEdge(v);
            if (e == null) {
                break;
            }
            hierarchy.unpackBackward(e, vertexList, edgeList);
            v = hierarchy.getContractionGraph().getEdgeSource(e);
        }
        // traverse reverse path
        v = commonVertex;
        while (true) {
            ContractionEdge<E> e = backwardFrontier.getTreeEdge(v);

            if (e == null) {
                break;
            }

            hierarchy.unpackForward(e, vertexList, edgeList);
            v = hierarchy.getContractionGraph().getEdgeTarget(e);
        }
        return new GraphWalk<>(graph, source.vertex, sink.vertex, vertexList, edgeList, weight);
    }

    protected GraphPath<V, E> createPath(
            BaseSearchFrontier<V, E> forwardFrontier, BaseSearchFrontier<V, E> backwardFrontier,
            double weight, V source, V commonVertex, V sink)
    {
        LinkedList<E> edgeList = new LinkedList<>();
        LinkedList<V> vertexList = new LinkedList<>();

        // add common vertex
        vertexList.add(commonVertex);

        // traverse forward path
        V v = commonVertex;
        while (true) {
            E e = forwardFrontier.getTreeEdge(v);

            if (e == null) {
                break;
            }

            edgeList.addFirst(e);
            v = Graphs.getOppositeVertex(forwardFrontier.graph, e, v);
            vertexList.addFirst(v);
        }

        // traverse reverse path
        v = commonVertex;
        while (true) {
            E e = backwardFrontier.getTreeEdge(v);

            if (e == null) {
                break;
            }

            edgeList.addLast(e);
            v = Graphs.getOppositeVertex(backwardFrontier.graph, e, v);
            vertexList.addLast(v);
        }

        return new GraphWalk<>(graph, source, sink, vertexList, edgeList, weight);
    }
}
