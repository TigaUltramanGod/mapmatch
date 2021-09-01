package point;


import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @description: This is a trajectory after MapMatching
 * @date : 2019-03-01 20:15
 **/
public class MapMatchedTrajectory implements Serializable {
    /**
     * 对象ID
     */
    private final String objectID;
    /**
     * 地图匹配后的点
     */
    private final List<MapMatchedPoint> mmPtList;

    /**
     * 构造函数
     *
     * @param objectID 对象id
     * @param mmPtList 匹配点list
     */
    public MapMatchedTrajectory(String objectID, List<MapMatchedPoint> mmPtList) {
        this.objectID = objectID;
        this.mmPtList = mmPtList;
    }

    public String getObjectID() {
        return objectID;
    }

    public List<MapMatchedPoint> getMmPtList() {
        return mmPtList;
    }


    /**
     * 生成匹配点LineString
     *
     * @return LineString
     */
    public LineString getCandidateLineString() {
        if (mmPtList != null) {
            int srid = 4326;
            return new LineString(new CoordinateArraySequence(this.mmPtList.stream().filter(i -> i.getCandidatePoint() != null).map(mmPoint ->
                    new Coordinate(mmPoint.getCandidatePoint().getLon(), mmPoint.getCandidatePoint().getLat())).collect(Collectors.toList()).toArray(new Coordinate[]{}))
                    , new GeometryFactory(new PrecisionModel(), srid));
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return getCandidateLineString().toString();
    }

}
