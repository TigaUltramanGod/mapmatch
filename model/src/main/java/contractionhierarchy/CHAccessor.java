package contractionhierarchy;

import org.apache.spark.model.st.spatial.SpatialCoord;
import org.apache.spark.model.st.spatial.graph.RoadNetwork;
import org.apache.spark.model.st.spatial.graph.RoadNode;
import org.apache.spark.model.st.spatial.graph.RoadSegment;
import org.jgrapht.alg.shortestpath.ContractionHierarchyPrecomputation;
import org.jgrapht.alg.shortestpath.ContractionHierarchyPrecomputation.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class CHAccessor {


    public static void write(RoadNetwork roadNetwork, String path) throws IOException {
        ContractionHierarchyPrecomputation<RoadNode, RoadSegment> preComputation = new ContractionHierarchyPrecomputation<>(roadNetwork.getRoadGraph(true));
        ContractionHierarchy<RoadNode, RoadSegment> contractionHierarchy = preComputation.computeContractionHierarchy();

        BufferedWriter bw = new BufferedWriter(new FileWriter(path));
        final List<CHRoadSegment> chRoadSegments = CHTransformer.graph2Edges(contractionHierarchy);
        for (CHRoadSegment rs : chRoadSegments) {
            bw.write(rs.toString());
            bw.newLine();
        }
        bw.close();
    }

    public static CHRoadSegment read(String string) {
        final String[] elements = string.split("\\|");
        int chRoadSegmentId = Integer.parseInt(elements[0]);
        ContractionVertex<RoadNode> sourceCHNode = parseNode(elements[1]);
        ContractionVertex<RoadNode> targetCHNode = parseNode(elements[2]);
        double weight = Double.parseDouble(elements[3]);
        boolean isUpward = Boolean.parseBoolean(elements[4]);
        int originRoadSegmentId = Integer.parseInt(elements[5]);
        int referenceCHFirstEdgeId = Integer.parseInt(elements[6]);
        int referenceCHSecondEdgeId = Integer.parseInt(elements[7]);
        int originalEdgeCount = Integer.parseInt(elements[8]);
        return new CHRoadSegment(chRoadSegmentId, sourceCHNode, targetCHNode, weight, isUpward, originRoadSegmentId, referenceCHFirstEdgeId, referenceCHSecondEdgeId, originalEdgeCount);
    }

    private static ContractionVertex<RoadNode> parseNode(String string) {
        final String[] elements = string.split(",");
        int vertexId = Integer.parseInt(elements[0]);
        int nodeId = Integer.parseInt(elements[1]);
        double lng = Double.parseDouble(elements[2]);
        double lat = Double.parseDouble(elements[3]);
        int level = Integer.parseInt(elements[4]);
        ContractionVertex<RoadNode> node = new ContractionVertex<RoadNode>(new RoadNode(nodeId, new SpatialCoord(lng, lat)), vertexId);
        node.setContractionLevel(level);
        return node;
    }
}
