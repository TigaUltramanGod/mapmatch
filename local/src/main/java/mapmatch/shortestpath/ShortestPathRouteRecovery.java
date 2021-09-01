package mapmatch.shortestpath;

import org.apache.spark.model.st.RouteOfTrajectory;
import org.apache.spark.model.st.SubRoute;
import org.apache.spark.model.st.spatial.SpatialLine;
import org.apache.spark.model.st.spatial.graph.RoadNetwork;
import org.apache.spark.model.st.spatial.graph.RoadNode;
import org.apache.spark.model.st.spatial.graph.RoadSegment;
import org.locationtech.jts.geom.GeometryFactory;
import point.MapMatchedPoint;
import point.MapMatchedTrajectory;
import scala.Option;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Description:
 * Warning: the first edge enter time, and the last edge leave time is not accurate!
 *
 * @date : 2018/03/25
 */
public class ShortestPathRouteRecovery  {
    /**
     * algo
     */
    private final AbstractShortestPathAlgo pathAlgo;


    public ShortestPathRouteRecovery(AbstractShortestPathAlgo pathAlgo ) {
        this.pathAlgo = pathAlgo;
    }

    public List<RouteOfTrajectory> recover(MapMatchedTrajectory mmTraj, RoadNetwork roadNetwork) {
        List<MapMatchedPoint> rawMmPts = mmTraj.getMmPtList();
        List<MapMatchedPoint> mmPts = new ArrayList<>();
        // 去除null的map match point
        for (MapMatchedPoint mapMatchedPoint : rawMmPts) {
            if (mapMatchedPoint.getCandidatePoint() != null) {
                mmPts.add(mapMatchedPoint);
            }
        }
        List<SubRoute> routeEntities = new ArrayList<>();
        Timestamp prePtEdgeLeaveTime, prePtEdgeEnterTime, curPtEdgeEnterTime;
        List<RouteOfTrajectory> routes = new ArrayList<>();
        // 只有一个点匹配到了，其他map match point的candidate point 因为是null被filter掉了
        if (mmPts.size() == 1) {
            MapMatchedPoint mmPt = mmPts.get(0);
            RoadSegment roadSegment = roadNetwork.getRoadSegment(mmPt.getCandidatePoint().getRoadSegmentID());
            // km/h转为 m/s
            double speed = roadSegment.getSpeedLimit() / 3.6;
            // 无法推测进入和离开时间，因此将进入和离开时间由映射到的路段的限速来计算
            Timestamp enterTime = Timestamp.from(mmPt.getTime().toInstant().minusSeconds(new Double(mmPt.getCandidatePoint().getOffset() / speed).longValue()));
            Timestamp leaveTime = Timestamp.from(mmPt.getTime().toInstant().plusSeconds(new Double((roadSegment.getLength() - mmPt.getCandidatePoint().getOffset()) / speed).longValue()));
            SpatialLine spatialLine = new SpatialLine(roadSegment.getCoordSequence(), new GeometryFactory());
            routeEntities.add(new SubRoute(roadSegment.getRoadId(), enterTime, Option.apply(spatialLine)));
            RouteOfTrajectory route = new RouteOfTrajectory(mmTraj.getObjectID(), routeEntities.toArray(new SubRoute[0]), leaveTime, true);
            routes.add(route);
        } else if (mmPts.size() > 1) {
            // assume the first edge enter time is the first point appear time
            prePtEdgeEnterTime = mmPts.get(0).getTime();
            for (int idx = 1; idx < mmPts.size(); idx++) {
                MapMatchedPoint preMMPt = mmPts.get(idx - 1);
                MapMatchedPoint curMMPt = mmPts.get(idx);
                RoadSegment preRoadSegment = roadNetwork.getRoadSegment(preMMPt.getCandidatePoint().getRoadSegmentID());
                RoadSegment curRoadSegment = roadNetwork.getRoadSegment(curMMPt.getCandidatePoint().getRoadSegmentID());
                // only check two points on the different road
                if (!preRoadSegment.equals(curRoadSegment)) {
                    Tuple2<Double, List<RoadSegment>> gp = pathAlgo.findShortestPathGraph(
                            getStartNode(preMMPt, preRoadSegment), getEndNode(curMMPt, curRoadSegment));
                    if (gp._2 == null) {
                        // if map matching is correct, every two road segment can be connect
                        // if can't connect, we split the route into two routes
                        // in this case, the actual previous edge leave time cannot be calculated
                        // we estimate it using the time of the next point
                        prePtEdgeLeaveTime = preMMPt.getTime();
                        RoadSegment tempRs = roadNetwork.getRoadSegment(preMMPt.getCandidatePoint().getRoadSegmentID());
                        routeEntities.add(
                                new SubRoute(tempRs.getRoadId(), prePtEdgeEnterTime, Option.apply(new SpatialLine(tempRs.getCoordSequence(), new GeometryFactory()))));
                        RouteOfTrajectory preRoute = new RouteOfTrajectory(mmTraj.getObjectID(), routeEntities.toArray(new SubRoute[0]), prePtEdgeLeaveTime, true);
                        routes.add(preRoute);
                        routeEntities = new ArrayList<>();
                        // in this case, the actual next edge enter time cannot be calculated,
                        // we estimate next edge enter time as the current point time
                        prePtEdgeEnterTime = curMMPt.getTime();
                        continue;
                    }
                    long totalTimeSpan = curMMPt.getTime().getTime() - preMMPt.getTime().getTime();
                    double preMatchedPtOffset = preMMPt.getCandidatePoint().getOffset();
                    double curMatchedPtOffset = curMMPt.getCandidatePoint().getOffset();
                    double totalDistance = gp._1 +
                            (preRoadSegment.getLength() - preMatchedPtOffset) + curMatchedPtOffset;
                    prePtEdgeLeaveTime = Timestamp.from(preMMPt.getTime().toInstant().plusMillis(
                            (long) ((preRoadSegment.getLength() - preMatchedPtOffset) *
                                    totalTimeSpan / totalDistance)));
                    routeEntities.add(new SubRoute(preRoadSegment.getRoadId(), prePtEdgeEnterTime, Option.apply(new SpatialLine(preRoadSegment.getCoordSequence(), new GeometryFactory()))));
                    curPtEdgeEnterTime = Timestamp.from(curMMPt.getTime().toInstant().minusMillis(
                            (long) (curMatchedPtOffset * totalTimeSpan / totalDistance)));

                    if (gp._2.isEmpty()) {
                        // to make sure the last connect edge leave time meets cur point enter time due to double calculation accuracy
                        curPtEdgeEnterTime = prePtEdgeLeaveTime;
                    } else {
                        List<SubRoute> interpolatedPart = linearInterpolateRoute(gp, prePtEdgeLeaveTime, curPtEdgeEnterTime);
                        routeEntities.addAll(interpolatedPart);
                    }
                    // enter time is updated only when a new edge comes
                    prePtEdgeEnterTime = curPtEdgeEnterTime;
                }
            }
            // add the last edge
            // assume the last edge leave time is the last point appear time
            RoadSegment lastRs = roadNetwork.getRoadSegment(mmPts.get(mmPts.size() - 1).getCandidatePoint().getRoadSegmentID());
            routeEntities.add(
                    new SubRoute(lastRs.getRoadId(),
                            prePtEdgeEnterTime, Option.apply(new SpatialLine(lastRs.getCoordSequence(), new GeometryFactory()))));
            routes.add(new RouteOfTrajectory(mmTraj.getObjectID(), routeEntities.toArray(new SubRoute[0]), mmPts.get(mmPts.size() - 1).getTime(), true));
        } else {
            return null;
        }
        return routes;
    }

    /**
     * @param gp            图
     * @param pathEnterTime 进入时间
     * @param pathLeaveTime 离开时间
     * @return List<SegmentInTrajectory> 轨迹中路段list
     */
    private List<SubRoute> linearInterpolateRoute(Tuple2<Double, List<RoadSegment>>  gp, Timestamp pathEnterTime,
                                                  Timestamp pathLeaveTime) {
        List<RoadSegment> path = gp._2;
        List<SubRoute> routeEntities = new ArrayList<>();
        long pathTimeSpan = pathLeaveTime.getTime() - pathEnterTime.getTime();
        Timestamp edgeEnterTime = pathEnterTime;
        Timestamp edgeLeaveTime;
        for (int j = 0; j < path.size(); j++) {
            if (j == path.size() - 1) {
                // to make sure the last connect edge leave time
                // meet cur point enter time due to double calculation accuracy
                edgeLeaveTime = pathLeaveTime;
            } else {
                edgeLeaveTime = Timestamp.from(edgeEnterTime.toInstant().plusMillis((long) (path.get(j).getLength()
                        * pathTimeSpan / gp._1)));
            }
            RoadSegment tempRs = path.get(j);
            routeEntities.add(new SubRoute(tempRs.getRoadId(), edgeEnterTime, Option.apply(new SpatialLine(tempRs.getCoordSequence(), new GeometryFactory()))));
            edgeEnterTime = edgeLeaveTime;
        }
        return routeEntities;
    }

    private RoadNode getStartNode(MapMatchedPoint pt, RoadSegment roadSegment) {
        if (pt.getCandidatePoint().getOffset() == 0.0) {
            return roadSegment.getStartNode();
        }
        return roadSegment.getEndNode();
    }

    private RoadNode getEndNode(MapMatchedPoint pt, RoadSegment roadSegment) {
        if (pt.getCandidatePoint().getOffset() == roadSegment.getLength()) {
            return roadSegment.getEndNode();
        }
        return roadSegment.getStartNode();
    }
}
