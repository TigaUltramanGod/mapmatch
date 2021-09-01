package point;

import org.apache.spark.model.st.STPoint;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * @description:
 * @date : 2019-03-01 20:27
 **/
public class MapMatchedPoint implements Serializable {
    /**
     * 原始点
     */
    private final STPoint rawPoint;
    /**
     * 候选点
     */
    private final CandidatePoint candidatePoint;

    /**
     * 构造函数
     *
     * @param rawPoint       原始点
     * @param candidatePoint 匹配点
     */
    public MapMatchedPoint(STPoint rawPoint, CandidatePoint candidatePoint) {
        this.rawPoint = rawPoint;
        this.candidatePoint = candidatePoint;
    }

    public STPoint getRawPoint() {
        return rawPoint;
    }

    /**
     * @return ZonedDateTime
     */
    public Timestamp getTime() {
        return this.rawPoint.getTime();
    }

    public CandidatePoint getCandidatePoint() {
        return candidatePoint;
    }

    @Override
    public int hashCode() {
        String code = String.format("%d%d", this.getCandidatePoint().hashCode(), this.getTime().hashCode());
        char[] charArr = code.toCharArray();
        int hashcode = 0;
        for (char c : charArr) {
            hashcode = hashcode * 131 + c;
        }
        return hashcode;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof MapMatchedPoint && ((MapMatchedPoint) o).getCandidatePoint().equals(getCandidatePoint()) && ((MapMatchedPoint) o).getTime().equals(getTime());
    }
}
