package mapmatch.tihmm;

import mapmatch.shortestpath.ShortestPathAlgoTypeEnum;
import mapmatch.shortestpath.multiple.shortestpath.AbstractMultipleShortestPathAlgo;
import org.apache.spark.model.st.spatial.graph.RoadNetwork;
import org.apache.spark.model.st.spatial.graph.RoadNode;
import org.apache.spark.model.st.spatial.graph.RoadSegment;
import point.CandidatePoint;
import point.GeoFunction;
import scala.Tuple2;

import java.util.Map;

public class TiHmmMultipleMatcher extends TiHmmMapMatcher {

    public TiHmmMultipleMatcher(RoadNetwork roadNetwork, ShortestPathAlgoTypeEnum algoType) throws Exception {
        super(roadNetwork, algoType);
        this.pathAlgo = AbstractMultipleShortestPathAlgo.getAlgo(roadNetwork, algoType);
    }

    public TiHmmMultipleMatcher(RoadNetwork roadNetwork) throws Exception {
        this(roadNetwork, ShortestPathAlgoTypeEnum.ASTAR);
        this.pathAlgo = AbstractMultipleShortestPathAlgo.getAlgo(roadNetwork, ShortestPathAlgoTypeEnum.ASTAR);
    }

    @Override
    protected void computeTransitionProbabilities(TimeStep prevTimeStep, TimeStep timeStep, HmmProbabilities probabilities) {
        final double linearDist = GeoFunction.getDistanceInM(prevTimeStep.getObservation(), timeStep.getObservation());
        final Map<Tuple2<RoadNode, RoadNode>, Double> path = ((AbstractMultipleShortestPathAlgo) pathAlgo).findAllPath(prevTimeStep.getCandidates(), timeStep.getCandidates(), roadNetwork);

        for (CandidatePoint preCandiPt : prevTimeStep.getCandidates()) {
            for (CandidatePoint curCandiPt : timeStep.getCandidates()) {
                RoadSegment preRs = roadNetwork.getRoadSegment(preCandiPt.getRoadSegmentID());
                RoadNode preV = preRs.getEndNode();
                RoadNode curU = roadNetwork.getRoadSegment(curCandiPt.getRoadSegmentID()).getStartNode();
                double dist;
                try {
                    dist = path.getOrDefault(new Tuple2<>(preV, curU), Double.POSITIVE_INFINITY);
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
