package point;

import org.apache.spark.model.st.spatial.SpatialPoint;

import java.io.Serializable;

/**
 * Description:
 *
 * @date : 2020/09/02
 */
public class ProjectionPoint extends SpatialPoint implements Serializable {
    /**
     * 误差距离，单位为m
     */
    private final double errorDistance;
    /**
     * 投影的斜率（具体含义参考GeoFunction类）
     */
    private final double rate;

    /**
     * 构造函数
     *
     * @param projectionPoint 映射点
     * @param errorDistance 匹配误差
     */
    public ProjectionPoint(SpatialPoint projectionPoint, double errorDistance, double rate) {
        super(projectionPoint.getLon(),projectionPoint.getLat());
        this.errorDistance = errorDistance;
        this.rate = rate;
    }

    public double getErrorDistance() {
        return errorDistance;
    }

    public double getRate() {
        return rate;
    }
}
