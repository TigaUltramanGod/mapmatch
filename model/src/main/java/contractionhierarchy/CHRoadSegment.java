package contractionhierarchy;

import org.apache.spark.model.st.spatial.graph.RoadNode;
import org.jgrapht.alg.shortestpath.ContractionHierarchyPrecomputation.*;
import org.locationtech.jts.geom.Polygon;

import java.io.Serializable;

/**
 * Store the necessary information to construct a ContractionHierarchy Edge
 *
 * @date 2021/06/08
 */
public class CHRoadSegment implements Serializable {
    /**
     * CH Edge Id
     */
    private final int chRoadSegmentId;

    /**
     * source CHNode of this CH Edge
     */
    private final ContractionVertex<RoadNode> sourceCHNode;

    /**
     * target CHNode of this CH Edge
     */
    private final ContractionVertex<RoadNode> targetCHNode;

    /**
     * weight of this edge
     */
    private final double weight;

    /**
     * if upward, it will be chosen in CHBidirectionalDijkstra
     */
    private final boolean isUpward;

    /**
     * if this CH Edge covers only one RoadSegment on road network, this field store the id of which
     */
    private final int originRoadSegmentId;

    /**
     * if this CH Edge covers two CH edge, this field store the ID of first CH Edge
     */
    private final int referenceCHFirstEdgeId;

    /**
     * if this CH Edge covers two CH edges, this field store the ID of second CH Edge
     */
    private final int referenceCHSecondEdgeId;

    /**
     * number of edges that this CH edges cover
     */
    private final int originalEdgeCount;


    /**
     * @param chRoadSegmentId         CH Edge Id
     * @param sourceCHNode            source CHVertex of this CH Edge
     * @param targetCHNode            target CHVertex of this CH Edge
     * @param weight                  weight of this edge
     * @param isUpward                if upward, it will be chosen in CHBidirectionalDijkstra
     * @param originRoadSegmentId     if this CH Edge covers only one RoadSegment on road network, this field store the id of which
     * @param referenceCHFirstEdgeId  if this CH Edge covers two CH edge, this field store the ID of first CH Edge
     * @param referenceCHSecondEdgeId if this CH Edge covers two CH edge, this field store the ID of second CH Edge
     * @param originalEdgeCount       number of edges that this CH edges cover
     */

    public CHRoadSegment(int chRoadSegmentId, ContractionVertex<RoadNode> sourceCHNode, ContractionVertex<RoadNode> targetCHNode, double weight, boolean isUpward, int originRoadSegmentId, int referenceCHFirstEdgeId, int referenceCHSecondEdgeId, int originalEdgeCount) {
        this.chRoadSegmentId = chRoadSegmentId;
        this.sourceCHNode = sourceCHNode;
        this.targetCHNode = targetCHNode;
        this.weight = weight;
        this.isUpward = isUpward;
        this.originRoadSegmentId = originRoadSegmentId;
        this.referenceCHFirstEdgeId = referenceCHFirstEdgeId;
        this.referenceCHSecondEdgeId = referenceCHSecondEdgeId;
        this.originalEdgeCount = originalEdgeCount;
    }

    /**
     * Get ID of CH Edge
     *
     * @return ID
     */
    public int getChRoadSegmentId() {
        return chRoadSegmentId;
    }

    /**
     * Get Source CH Node
     *
     * @return ID
     */
    public ContractionVertex<RoadNode> getSourceCHNode() {
        return sourceCHNode;
    }

    /**
     * Get Target CH Node
     *
     * @return ID
     */
    public ContractionVertex<RoadNode> getTargetCHNode() {
        return targetCHNode;
    }

    /**
     * Get CH Edge Weight
     *
     * @return weight
     */
    public double getWeight() {
        return weight;
    }

    /**
     * Get isUpward
     *
     * @return isUpward
     */
    public boolean isUpward() {
        return isUpward;
    }

    /**
     * Get ID of original Road Segment
     *
     * @return ID
     */
    public int getOriginRoadSegmentId() {
        return originRoadSegmentId;
    }

    /**
     * Get ID of ReferenceCHFirstEdge
     *
     * @return ID
     */
    public int getReferenceCHFirstEdgeId() {
        return referenceCHFirstEdgeId;
    }

    /**
     * Get ID of ReferenceCHSecondEdge
     *
     * @return ID
     */
    public int getReferenceCHSecondEdgeId() {
        return referenceCHSecondEdgeId;
    }

    /**
     * Get number of original edges
     *
     * @return origin edge number
     */
    public int getOriginalEdgeCount() {
        return originalEdgeCount;
    }

    @Override
    public String toString() {
        return chRoadSegmentId + "|" +
                sourceCHNode.getVertexId() + "," + sourceCHNode.getVertex().toString() + "," + sourceCHNode.contractionLevel + "|" +
                targetCHNode.getVertexId() + "," + targetCHNode.getVertex().toString() + "," + targetCHNode.contractionLevel + "|" +
                weight + "|" + isUpward + "|" + originRoadSegmentId + "|" + referenceCHFirstEdgeId + "|" + referenceCHSecondEdgeId + "|" + originalEdgeCount;
    }
}
