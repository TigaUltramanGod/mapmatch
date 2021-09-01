package point;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import org.apache.spark.model.st.spatial.SpatialPoint;
import org.apache.spark.model.st.spatial.graph.RoadNetwork;
import org.apache.spark.model.st.spatial.graph.RoadSegment;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Description: the mapped position in the road segment of a trajectory point
 *
 * @date : 2018/05/04
 * @modified by : Haowen Zhu
 */
public class CandidatePoint extends SpatialPoint implements Serializable {
    /**
     * 误差距离，单位为m
     */
    private final double errorDistanceInMeter;
    /**
     * 匹配点到路段起始点的偏移，单位为m
     */
    private final double offset;
    /**
     * 匹配点对应的路网路段中的ID
     */
    private final int roadSegmentID;
    /**
     * 对应匹配到道路上的区间
     */
    private final int matchLinkIdx;
    /**
     * 点到路的最大候选距离, 单位为m
     */
    public static final double CANDIDATE_DISTANCE = 300;

    /**
     * @param matchedPt            匹配点
     * @param roadSegment          路段
     * @param matchLinkIdx         道路id
     * @param errorDistanceInMeter 误差距离
     */
    public CandidatePoint(SpatialPoint matchedPt, RoadSegment roadSegment, int matchLinkIdx,
                          double errorDistanceInMeter) {
        super(matchedPt.getLon(), matchedPt.getLat());
        this.offset = calOffset(roadSegment);
        this.roadSegmentID = roadSegment.getRoadId();
        this.matchLinkIdx = matchLinkIdx;
        this.errorDistanceInMeter = errorDistanceInMeter;
    }

    /**
     * 重载构造函数
     *
     * @param matchedPoint         原始点
     * @param roadSegmentID        路段映射ID
     * @param matchLinkIdx         idx
     * @param errorDistanceInMeter error
     * @param offset               offset
     */
    public CandidatePoint(SpatialPoint matchedPoint, int roadSegmentID, int matchLinkIdx, double errorDistanceInMeter, double offset) {
        super(matchedPoint.getLon(), matchedPoint.getLat());
        this.offset = offset;
        this.roadSegmentID = roadSegmentID;
        this.matchLinkIdx = matchLinkIdx;
        this.errorDistanceInMeter = errorDistanceInMeter;
    }

    /**
     * 计算匹配点到路段起始点的偏移，单位：米
     *
     * @return double 米
     */
    private double calOffset(RoadSegment roadSegment) {
        double offset = 0.0;
        if (matchLinkIdx > 0) {
            for (int j = 0; j < matchLinkIdx; j++) {
                SpatialPoint p1 = new SpatialPoint(roadSegment.getCoordinateN(j));
                SpatialPoint p2 = new SpatialPoint(roadSegment.getCoordinateN(j + 1));
                offset += GeoFunction.getDistanceInM(p1, p2);
            }
        }
        SpatialPoint point = new SpatialPoint(roadSegment.getCoordinateN(matchLinkIdx));
        offset += GeoFunction.getDistanceInM(point, this);
        return offset;
    }

    public int getRoadSegmentID() {
        return roadSegmentID;
    }

    public double getOffset() {
        return offset;
    }

    public int getMatchLinkIdx() {
        return matchLinkIdx;
    }

    public double getErrorDistanceInMeter() {
        return errorDistanceInMeter;
    }

    /**
     * 给定一个点 和一个搜索距离 返回range query的roadSegment
     *
     * @param pt          点
     * @param roadNetwork 路网索引
     * @param dist        搜索的距离
     * @return 这个点在搜索范围内，所有可能对应的 candidate points
     */
    public static List<CandidatePoint> getCandidatePoint(SpatialPoint pt, RoadNetwork roadNetwork, double dist) {
        Envelope mbr = GeoFunction.getExtendedMbr(pt, dist);
        Rectangle rec = Geometries.rectangleGeographic(mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY());
        Iterable<RoadSegment> roadSegmentIterable = roadNetwork.getRoadRtree().search(rec).map(Entry::value).toBlocking().toIterable();
        List<CandidatePoint> result = new ArrayList<>();
        roadSegmentIterable.forEach(roadSegment1 -> {
            CandidatePoint candiPt = calCandidatePoint(pt, roadSegment1);
            if (candiPt.errorDistanceInMeter <= dist) {
                result.add(candiPt);
            }
        });
        return !result.isEmpty() ? result : new ArrayList<>();
    }

    /**
     * 找到离原始点最近的candidate point
     *
     * @param pt          原始点
     * @param roadNetwork 路网索引
     * @param dist        搜索距离
     * @return candidate point
     */
    public static CandidatePoint getNearestCandidatePoint(SpatialPoint pt, RoadNetwork roadNetwork, double dist) {
        List<CandidatePoint> candidates = getCandidatePoint(pt, roadNetwork, dist);
        if (candidates.size() != 0) {
            return Collections.min(candidates, Comparator.comparingDouble(CandidatePoint::getErrorDistanceInMeter));
        } else {
            return null;
        }
    }

    /**
     * 给一个点，路网，加上对应的roadSegment，找出candidate point
     *
     * @param rawPoint 原始点
     * @param rs       路段
     * @return 对应在该路段上的candidate point
     */
    public static CandidatePoint calCandidatePoint(SpatialPoint rawPoint, RoadSegment rs) {
        List<SpatialPoint> coords = rs.getCoords();
        Tuple2<ProjectionPoint, Integer> tuple = GeoFunction.calProjection(rawPoint, coords, 0, coords.size() - 1);
        ProjectionPoint projectionPoint = tuple._1;
        int matchIndex = tuple._2;
        return new CandidatePoint(projectionPoint, rs, matchIndex, projectionPoint.getErrorDistance());
    }

    @Override
    public String toString() {
        return this.roadSegmentID + "|" + this.errorDistanceInMeter;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CandidatePoint && ((CandidatePoint) o).getRoadSegmentID() == (getRoadSegmentID()) && super.equals(o);
    }

    @Override
    public boolean equals(Geometry g) {
        return g instanceof CandidatePoint && Math.abs(((CandidatePoint) g).getRoadSegmentID()) == Math.abs(getRoadSegmentID()) && super.equals(g);
    }
}
