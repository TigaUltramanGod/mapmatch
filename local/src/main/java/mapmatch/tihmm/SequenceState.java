package mapmatch.tihmm;

import org.apache.spark.model.st.STPoint;
import point.CandidatePoint;

/**
 * 辅助class，保存状态信息
 *
 * @author : Haowen Zhu
 * @date : 2019/09/027
 */
public class SequenceState {
    /**
     * 原始point的candidate
     */
    private final CandidatePoint state;
    /**
     * 原始point
     */
    private final STPoint observation;

    /**
     * 构造函数
     * @param state 原始point的candidate
     * @param observation 原始point
     */
    public SequenceState(CandidatePoint state, STPoint observation) {
        this.state = state;
        this.observation = observation;
    }

    public CandidatePoint getState() {
        return state;
    }

    public STPoint getObservation() {
        return observation;
    }

}
