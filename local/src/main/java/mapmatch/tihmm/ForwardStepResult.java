package mapmatch.tihmm;


import point.CandidatePoint;


import java.util.HashMap;
import java.util.Map;

/**
 * 辅助class，保存顺向推演时的状态信息
 *
 * @author : Haowen Zhu
 * @date : 2019/09/027
 */

public class ForwardStepResult {
    /**
     * 更新candidate point 后新的概率
     */
    private final Map<CandidatePoint, Double> newMessage;
    /**
     * 根据新的candidate point 指向新的extended state
     */
    private final Map<CandidatePoint, ExtendedState> newExtendedStates;

    public ForwardStepResult(int numberStates) {
        newMessage = new HashMap<>(numberStates);
        newExtendedStates = new HashMap<>(numberStates);
    }

    public Map<CandidatePoint, Double> getNewMessage() {
        return newMessage;
    }

    public Map<CandidatePoint, ExtendedState> getNewExtendedStates() {
        return newExtendedStates;
    }
}
