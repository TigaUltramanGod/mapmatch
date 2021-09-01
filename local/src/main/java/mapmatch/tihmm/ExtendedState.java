package mapmatch.tihmm;

import org.apache.spark.model.st.STPoint;
import point.CandidatePoint;

/**
 * 辅助class，反向追溯时保存每一步状态信息
 *
 * @author : Haowen Zhu
 * @date : 2019/09/027
 */
public class ExtendedState {
    /**
     * candidate point
     */
    private final CandidatePoint state;
    /**
     * 反向指针，指向之前的extendedState
     */
    private final ExtendedState backPointer;
    /**
     * 原始轨迹点
     */
    private final STPoint observation;

    /**
     * 构造函数
     *
     * @param state                candidate point
     * @param backPointer          向后的指针
     * @param observation          原始point
     */
    public ExtendedState(CandidatePoint state, ExtendedState backPointer, STPoint observation) {
        this.state = state;
        this.backPointer = backPointer;
        this.observation = observation;
    }

    public CandidatePoint getState() {
        return state;
    }

    public ExtendedState getBackPointer() {
        return backPointer;
    }

    public STPoint getObservation() {
        return observation;
    }
}