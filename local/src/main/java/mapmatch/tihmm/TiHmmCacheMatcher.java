package mapmatch.tihmm;

import mapmatch.shortestpath.single.shortestpath.OneToOneAStar;
import org.apache.spark.model.st.spatial.graph.RoadNetwork;
import org.apache.spark.model.st.spatial.graph.RoadNode;
import org.apache.spark.model.st.spatial.graph.RoadSegment;
import point.CandidatePoint;
import point.GeoFunction;
import scala.Tuple2;

import java.util.Map;

public class TiHmmCacheMatcher extends TiHmmMapMatcher {

    Map<Tuple2<Integer, Integer>, Double> cachePathMap;

    public TiHmmCacheMatcher(RoadNetwork roadNetwork, Map<Tuple2<Integer, Integer>, Double> cachePathMap) {
        super(roadNetwork);
        this.cachePathMap = cachePathMap;
    }

    @Override
    protected void computeTransitionProbabilities(TimeStep prevTimeStep, TimeStep timeStep, HmmProbabilities probabilities) {
        final double linearDist = GeoFunction.getDistanceInM(prevTimeStep.getObservation(), timeStep.getObservation());

        for (CandidatePoint preCandiPt : prevTimeStep.getCandidates()) {
            for (CandidatePoint curCandiPt : timeStep.getCandidates()) {
                RoadSegment preRs = roadNetwork.getRoadSegment(preCandiPt.getRoadSegmentID());
                RoadNode preV = preRs.getEndNode();
                RoadNode curU = roadNetwork.getRoadSegment(curCandiPt.getRoadSegmentID()).getStartNode();
                double dist;
                try {
                    final Tuple2<Integer, Integer> key = new Tuple2<>(preV.nodeId(), curU.nodeId());
                    if (cachePathMap.containsKey(key)) {
                        dist = cachePathMap.get(key);
                    } else {
                        dist = new OneToOneAStar(roadNetwork.getRoadGraph(true)).findShortestPathGraph(preV, curU)._1;
                    }
                } catch (Exception e) {
                    dist = Double.POSITIVE_INFINITY;
                }
                if (dist != Double.POSITIVE_INFINITY) {
                    double distToSrc = preRs.getLength() - preCandiPt.getOffset();
                    double distToEnd = curCandiPt.getOffset();
                    timeStep.addTransitionLogProbability(preCandiPt, curCandiPt,
                            probabilities.transitionLogProbability(dist + distToSrc + distToEnd, linearDist));
                }
            }
        }
    }
}
