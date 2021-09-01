package mapmatch.tihmm;

import mapmatch.shortestpath.AbstractShortestPathAlgo;
import mapmatch.shortestpath.single.shortestpath.AbstractSingleShortestPathAlgo;
import mapmatch.shortestpath.ShortestPathAlgoTypeEnum;
import mapmatch.shortestpath.ShortestPathRouteRecovery;
import org.apache.spark.model.st.RouteOfTrajectory;
import org.apache.spark.model.st.STPoint;
import org.apache.spark.model.st.Trajectory;
import org.apache.spark.model.st.spatial.graph.RoadNetwork;
import org.apache.spark.model.st.spatial.graph.RoadSegment;
import point.CandidatePoint;
import point.GeoFunction;
import point.MapMatchedPoint;
import point.MapMatchedTrajectory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * TiHmm Match class
 *
 * @author : Haowen Zhu
 * @date : 2019/09/27
 */

public class TiHmmMapMatcher {
    /**
     * emission P用正态分布函数来模拟，sigma为正态分布的概率函数参数
     */
    private static final double measurementErrorSigma = 50.0;
    /**
     * transition p 的指数概率函数参数
     */
    private static final double transitionProbabilityBeta = 2.0;

    /**
     * 路网
     */
    protected final RoadNetwork roadNetwork;

    protected AbstractShortestPathAlgo pathAlgo;

    /**
     * 构造函数
     *
     * @param roadNetwork 路网
     */
    public TiHmmMapMatcher(RoadNetwork roadNetwork) {
        this(roadNetwork, ShortestPathAlgoTypeEnum.ASTAR);
    }

    public TiHmmMapMatcher(RoadNetwork roadNetwork, ShortestPathAlgoTypeEnum algoType) {
        this.roadNetwork = roadNetwork;
        pathAlgo = AbstractSingleShortestPathAlgo.getAlgo(roadNetwork, algoType);
    }

    /**
     * 实现抽象类的map match 方法
     *
     * @param traj 原始轨迹
     * @return map match后的轨迹
     */
    public MapMatchedTrajectory matchTrajToMapMatchedTraj(Trajectory traj) {
        List<SequenceState> seq = this.computeViterbiSequence(traj.getSTPointList());
        assert traj.getSTPointList().size() == seq.size();
        List<MapMatchedPoint> mapMatchedPointList = new ArrayList<>(seq.size());
        for (SequenceState ss : seq) {
            CandidatePoint candiPt = null;
            if (ss.getState() != null) {
                candiPt = ss.getState();
            }
            mapMatchedPointList.add(new MapMatchedPoint(ss.getObservation(), candiPt));
        }
        return new MapMatchedTrajectory(traj.getOid(), mapMatchedPointList);
    }

    public List<RouteOfTrajectory> matchTrajToRoute(MapMatchedTrajectory trajectory) {
        ShortestPathRouteRecovery recovery = new ShortestPathRouteRecovery(pathAlgo);
        List<RouteOfTrajectory> routeOfTrajectories = recovery.recover(trajectory, roadNetwork);
        if (null == routeOfTrajectories || routeOfTrajectories.isEmpty()) {
            return null;
        }
        return routeOfTrajectories;
    }

    /**
     * the path may cannot be fully connected, so return a collection, every path is continuous
     *
     * @param trajectory 原始轨迹
     * @return a collection of continuous path
     */
    public List<RouteOfTrajectory> matchTrajToRoute(Trajectory trajectory) throws Exception {
        MapMatchedTrajectory mmTraj = matchTrajToMapMatchedTraj(trajectory);
        if (mmTraj == null) {
            return null;
        }
        List<RouteOfTrajectory> routeOfTrajectories = matchTrajToRoute(mmTraj);
        if (null == routeOfTrajectories || routeOfTrajectories.isEmpty()) {
            return null;
        }
        return routeOfTrajectories;
    }

    /**
     * 建立一个time step
     *
     * @param pt 原始轨迹点
     * @return timestep
     */
    private TimeStep createTimeStep(STPoint pt) {
        TimeStep timeStep = null;
        List<CandidatePoint> candidates = CandidatePoint.getCandidatePoint(pt, roadNetwork, measurementErrorSigma);
        if (!candidates.isEmpty()) {
            timeStep = new TimeStep(pt, candidates);
        }
        return timeStep;
    }

    /**
     * 计算一个 Viterbi sequence
     *
     * @param ptList 原始轨迹ptList
     * @return 保存了每一步step的所有状态
     */
    private List<SequenceState> computeViterbiSequence(List<STPoint> ptList) {
        List<SequenceState> seq = new ArrayList<>();
        final HmmProbabilities probabilities = new HmmProbabilities(measurementErrorSigma, transitionProbabilityBeta);
        TiViterbi viterbi = new TiViterbi();
        TimeStep preTimeStep = null;
        int idx = 0;
        int nbPoints = ptList.size();
        while (idx < nbPoints) {
            TimeStep timeStep = this.createTimeStep(ptList.get(idx));
            if (timeStep == null) {
                seq.addAll(viterbi.computeMostLikelySequence());
                seq.add(new SequenceState(null, ptList.get(idx)));
                viterbi = new TiViterbi();
                preTimeStep = null;
            } else {
                this.computeEmissionProbabilities(timeStep, probabilities);
                if (preTimeStep == null) {
                    viterbi.startWithInitialObservation(timeStep.getObservation(), timeStep.getCandidates(), timeStep.getEmissionLogProbabilities());
                } else {
                    this.computeTransitionProbabilities(preTimeStep, timeStep, probabilities);
                    viterbi.nextStep(timeStep.getObservation(), timeStep.getCandidates(), timeStep.getEmissionLogProbabilities(), timeStep.getTransitionLogProbabilities());
                }
                if (viterbi.isBroken) {
                    seq.addAll(viterbi.computeMostLikelySequence());
                    viterbi = new TiViterbi();
                    viterbi.startWithInitialObservation(timeStep.getObservation(), timeStep.getCandidates(), timeStep.getEmissionLogProbabilities());
                }
                preTimeStep = timeStep;
            }
            idx += 1;
        }
        if (seq.size() < nbPoints) {
            seq.addAll(viterbi.computeMostLikelySequence());
        }
        return seq;
    }

    /**
     * 根据time step和概率分布函数计算emission P
     *
     * @param timeStep    timeStep
     * @param probability 建立好的概率分布函数
     */
    private void computeEmissionProbabilities(TimeStep timeStep, HmmProbabilities probability) {
        for (CandidatePoint candiPt : timeStep.getCandidates()) {
            final double dist = candiPt.getErrorDistanceInMeter();
            timeStep.addEmissionLogProbability(candiPt, probability.emissionLogProbability(dist));
        }
    }

    /**
     * 计算之前timeStep到当前timeStep的概率
     *
     * @param prevTimeStep  之前的timestep
     * @param timeStep      当前的timestep
     * @param probabilities 建立好的概率分布函数
     */
    protected void computeTransitionProbabilities(TimeStep prevTimeStep, TimeStep timeStep, HmmProbabilities probabilities) {
        final double linearDist = GeoFunction.getDistanceInM(prevTimeStep.getObservation(), timeStep.getObservation());
        for (CandidatePoint preCandiPt : prevTimeStep.getCandidates()) {
            for (CandidatePoint curCandiPt : timeStep.getCandidates()) {
                final Tuple2<Double, List<RoadSegment>> tuple2
                        = pathAlgo.findShortestPathByCandidatePoint(preCandiPt, curCandiPt, roadNetwork);
                if (tuple2._1 != Double.MAX_VALUE) {
                    timeStep.addTransitionLogProbability(preCandiPt, curCandiPt, probabilities.transitionLogProbability(tuple2._1, linearDist));
                }
            }
        }
    }

}
