package mapmatch.tihmm;


import org.apache.spark.model.st.STPoint;
import point.CandidatePoint;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 辅助class，保存状态信息
 *
 * @author : Haowen Zhu
 * @date : 2019/09/027
 */
public class TimeStep {
    /**
     * 原始轨迹点
     */
    private final STPoint observation;
    /**
     * 轨迹点对应的candidate point列表
     */
    private final List<CandidatePoint> candidates;
    /**
     * 每一个candidate point对应的emission概率
     */
    private final Map<CandidatePoint, Double> emissionLogProbabilities = new HashMap<>();
    /**
     * <candidatePt,candidatePt>为索引 (transition) Tuple2<fromCandidatePoint, toCandidatePoint>
     */
    private final Map<Tuple2<CandidatePoint,CandidatePoint>, Double> transitionLogProbabilities = new HashMap<>();

    /**
     * @param observation point
     * @param candidates  candidate point
     */
    TimeStep(STPoint observation, List<CandidatePoint> candidates) {
        if (observation == null || candidates == null) {
            throw new NullPointerException("Observation and candidates must not be null.");
        }
        this.observation = observation;
        this.candidates = candidates;
    }

    /**
     * 添加 emission 概率
     * @param candidate candidate point
     * @param emissionLogProbability 对应的emission概率
     */
    void addEmissionLogProbability(CandidatePoint candidate, Double emissionLogProbability) {
        if (emissionLogProbabilities.containsKey(candidate)) {
            throw new IllegalArgumentException("Candidate has already been added.");
        }
        emissionLogProbabilities.put(candidate, emissionLogProbability);
    }

    /**
     * 添加transition概率
     * @param fromPosition 之前的candidate point
     * @param toPosition 当前的candidate point
     * @param transitionLogProbability 给定的transition概率
     */
    void addTransitionLogProbability(CandidatePoint fromPosition, CandidatePoint toPosition, Double transitionLogProbability) {
        final Tuple2<CandidatePoint, CandidatePoint> transition = new Tuple2<>(fromPosition, toPosition);
        if (transitionLogProbabilities.containsKey(transition)) {
            throw new IllegalArgumentException("Transition has already been added.");
        }
        transitionLogProbabilities.put(transition, transitionLogProbability);
    }


    STPoint getObservation() {
        return observation;
    }

    List<CandidatePoint> getCandidates() {
        return candidates;
    }

    Map<CandidatePoint, Double> getEmissionLogProbabilities() {
        return emissionLogProbabilities;
    }

    Map<Tuple2<CandidatePoint, CandidatePoint>, Double> getTransitionLogProbabilities() {
        return transitionLogProbabilities;
    }
}
