package contractionhierarchy;

import org.apache.spark.model.st.spatial.graph.RoadGraph;
import org.apache.spark.model.st.spatial.graph.RoadNetwork;
import org.apache.spark.model.st.spatial.graph.RoadNode;
import org.apache.spark.model.st.spatial.graph.RoadSegment;
import org.jgrapht.Graph;
import org.jgrapht.alg.shortestpath.ContractionHierarchyPrecomputation.ContractionEdge;
import org.jgrapht.alg.shortestpath.ContractionHierarchyPrecomputation.ContractionHierarchy;
import org.jgrapht.alg.shortestpath.ContractionHierarchyPrecomputation.ContractionVertex;
import org.jgrapht.alg.util.Pair;
import org.jgrapht.graph.builder.GraphTypeBuilder;

import java.util.*;

public class CHTransformer {

    /**
     * transform the contraction graph to list of ch roads in order to store as table in just
     * every ch roads has spatial attribute(mbr) for the indexing
     */
    public static List<CHRoadSegment> graph2Edges(ContractionHierarchy<RoadNode, RoadSegment> contractionHierarchy) {
        Graph<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>> chGraph = contractionHierarchy.getContractionGraph();
        List<CHRoadSegment> chRoadSegmentList = new ArrayList<>(chGraph.edgeSet().size());
        Map<ContractionEdge<RoadSegment>, Integer> edgeIDMapping = new HashMap<>(chGraph.edgeSet().size());
        List<ContractionEdge<RoadSegment>> edgeList = new ArrayList<>(chGraph.edgeSet());
        edgeList.sort(Comparator.comparingInt(ContractionEdge::getOriginalEdges));

        int idx = 0;
        for (ContractionEdge<RoadSegment> originalEdge : edgeList) {
            int originRoadSegmentId;
            int referenceEdgeId1;
            int referenceEdgeId2;
            // represent the original roadSegment itself, not contracted
            if (originalEdge.getOriginalEdges() == 1) {
                originRoadSegmentId = originalEdge.getEdge().getRoadId();
                referenceEdgeId1 = -1;
                referenceEdgeId2 = -1;
            }
            // contracted, may reference one or two CHedges
            else {
                originRoadSegmentId = -1;
                Pair<ContractionEdge<RoadSegment>, ContractionEdge<RoadSegment>> pairEdge = originalEdge.getBypassedEdges();
                referenceEdgeId1 = edgeIDMapping.get(pairEdge.getFirst());
                referenceEdgeId2 = edgeIDMapping.get(pairEdge.getSecond());
            }
            CHRoadSegment chRoadSegment = new CHRoadSegment(idx, chGraph.getEdgeSource(originalEdge), chGraph.getEdgeTarget(originalEdge),
                    chGraph.getEdgeWeight(originalEdge), originalEdge.isUpward(),
                    originRoadSegmentId, referenceEdgeId1, referenceEdgeId2, originalEdge.getOriginalEdges());
            chRoadSegmentList.add(chRoadSegment);
            edgeIDMapping.put(originalEdge, idx);
            idx++;
        }
        return chRoadSegmentList;
    }

    /**
     * transform the list of ch roads stored as table in just to ch graph object in the memory
     */
    public static ContractionHierarchy<RoadNode, RoadSegment> edges2Graph(List<CHRoadSegment> chRoadSegmentList, RoadNetwork roadNetwork) {
        RoadGraph roadGraph = roadNetwork.getRoadGraph(true);
        Graph<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>> chGraph =
                GraphTypeBuilder.<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>>directed().weighted(true)
                        .allowingMultipleEdges(true).allowingSelfLoops(false).buildGraph();
        Map<RoadNode, ContractionVertex<RoadNode>> nodeMapping = new HashMap<>(roadGraph.vertexSet().size());

        chRoadSegmentList.sort(Comparator.comparingInt(CHRoadSegment::getOriginalEdgeCount));
        Map<Integer, ContractionEdge<RoadSegment>> chEdgeMap = new HashMap<>(chRoadSegmentList.size());

        for (CHRoadSegment chRoadSegment : chRoadSegmentList) {
            ContractionEdge<RoadSegment> curEdge;
            // not contracted, chEdge represents the original roadSegment
            if (chRoadSegment.getOriginalEdgeCount() == 1) {
                curEdge = new ContractionEdge<>(roadNetwork.getRoadSegment(chRoadSegment.getOriginRoadSegmentId()));
            } else {
                ContractionEdge<RoadSegment> firstEdge = chEdgeMap.get(chRoadSegment.getReferenceCHFirstEdgeId());
                ContractionEdge<RoadSegment> secondEdge = chEdgeMap.get(chRoadSegment.getReferenceCHSecondEdgeId());
                curEdge = new ContractionEdge<>(new Pair<>(firstEdge, secondEdge));

            }
            curEdge.setUpward(chRoadSegment.isUpward());
            ContractionVertex<RoadNode> sourceCHNode = chRoadSegment.getSourceCHNode();
            ContractionVertex<RoadNode> targetCHNode = chRoadSegment.getTargetCHNode();
            addNode(nodeMapping, chGraph, sourceCHNode);
            addNode(nodeMapping, chGraph, targetCHNode);
            chGraph.addEdge(sourceCHNode, targetCHNode, curEdge);
            chGraph.setEdgeWeight(curEdge, chRoadSegment.getWeight());
            chEdgeMap.put(chRoadSegment.getChRoadSegmentId(), curEdge);
            nodeMapping.putIfAbsent(sourceCHNode.getVertex(), sourceCHNode);
            nodeMapping.putIfAbsent(targetCHNode.getVertex(), targetCHNode);
        }
        return new ContractionHierarchy<>(roadGraph, chGraph, nodeMapping);
    }

    private static void addNode(Map<RoadNode, ContractionVertex<RoadNode>> nodeMapping,
                                Graph<ContractionVertex<RoadNode>, ContractionEdge<RoadSegment>> chGraph,
                                ContractionVertex<RoadNode> node) {
        if (!nodeMapping.containsKey(node.getVertex())) {
            nodeMapping.put(node.getVertex(), node);
            chGraph.addVertex(node);
        }
    }

}
