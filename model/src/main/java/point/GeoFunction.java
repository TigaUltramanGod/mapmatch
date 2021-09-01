package point;

import org.apache.spark.model.st.STPoint;
import org.apache.spark.model.st.spatial.SpatialPoint;
import org.locationtech.jts.geom.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class GeoFunction {

    /**
     * 地球长半径
     */
    private static final double EARTH_RADIUS_IN_METER = 6378137.0;

    /**
     * 计算两经纬度间距离
     *
     * @param lng1 起始点经度
     * @param lat1 起始点纬度
     * @param lng2 终点经度
     * @param lat2 终点纬度
     * @return kilometer
     */
    public static double getDistanceInKM(double lng1, double lat1, double lng2, double lat2) {
        return getDistanceInM(lng1, lat1, lng2, lat2) / 1000.0;
    }

    /**
     * 计算两经纬度间距离
     *
     * @param p1 第一个点
     * @param p2 第二个点
     * @return double kilometer
     */
    public static double getDistanceInKM(Point p1, Point p2) {
        return getDistanceInKM(p1.getX(), p1.getY(), p2.getX(), p2.getY());
    }

    /**
     * 计算两经纬度间距离
     *
     * @param lng1 起始点经度
     * @param lat1 起始点纬度
     * @param lng2 终点经度
     * @param lat2 终点纬度
     * @return meter
     */
    public static double getDistanceInM(double lng1, double lat1, double lng2, double lat2) {
        double radLat1 = Math.toRadians(lat1);
        double radLat2 = Math.toRadians(lat2);
        double radLatDistance = radLat1 - radLat2;
        double radLngDistance = Math.toRadians(lng1) - Math.toRadians(lng2);
        return 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(radLatDistance / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(radLngDistance / 2), 2))) * EARTH_RADIUS_IN_METER;
    }

    /**
     * 米勒坐标投影，将经纬度坐标转平面坐标
     * 文档链接 ：https://blog.csdn.net/qq_31100961/article/details/52331708?locationNum=2&fps=1
     *
     * @param lng lng
     * @param lat lat
     * @return tuple，第一个元素是lng，第二个元素是lat
     */
    public static double[] gcsToMiller(double lng, double lat) {
        double width = 2 * Math.PI * EARTH_RADIUS_IN_METER;
        double height = 0.5 * width;
        double mill = 2.3;
        double x = Math.toRadians(lng);
        double y = Math.toRadians(lat);
        y = 1.25 * Math.log(Math.tan(0.25 * Math.PI + 0.4 * y));
        x = (width / 2) + (width / (2 * Math.PI)) * x;
        y = (height / 2) - (height / (2 * mill)) * y;
        return new double[]{x, y};
    }

    /**
     * 计算两经纬度间距离
     *
     * @param p1 第一个点
     * @param p2 第二个点
     * @return double meter
     */
    public static double getDistanceInM(Point p1, Point p2) {
        return getDistanceInM(p1.getX(), p1.getY(), p2.getX(), p2.getY());
    }

    /**
     * 计算一个 point list 对应的长度, 单位为M
     *
     * @param points point list
     * @return 长度
     */
    public static double getDistanceInM(List<SpatialPoint> points) {
        double dist = 0;
        if (points.size() < 2) {
            return dist;
        }
        for (int i = 1; i < points.size(); i++) {
            dist += GeoFunction.getDistanceInM(points.get(i), points.get(i - 1));
        }
        return dist;
    }

    /**
     * 将球面距离转化为度（在地理坐标系下做缓冲区时使用）
     * 此方法与经纬度转距离的前提一致，即将地球抽象为规则球体
     * 注意:此方法南北向无误差，东西向缓冲距离略小
     *
     * @param distance 距离，单位m
     * @return 弧度
     */
    public static double getDegreeFromM(double distance) {
        double perimeter = 2 * Math.PI * EARTH_RADIUS_IN_METER;
        double degreePerM = 360 / perimeter;
        return distance * degreePerM;
    }

    /**
     * 计算两点之间的欧几里得距离
     *
     * @param point1 第一个点
     * @param point2 第二个点
     * @return double 欧几里得距离
     */
    public static double getEuclideanDis(SpatialPoint point1, SpatialPoint point2) {
        double x = point1.getLon() - point2.getLon();
        double y = point1.getLat() - point2.getLat();
        return Math.sqrt(x * x + y * y);
    }

    /**
     * 计算两点之间的欧几里得距离
     *
     * @param lng1 经度
     * @param lat1 纬度
     * @param lng2 经度
     * @param lat2 纬度
     * @return 返回欧氏距离，注意这里返回的还是经纬度弧度数
     */
    public static double getEuclideanDis(double lng1, double lat1, double lng2, double lat2) {
        double x = lng1 - lng2;
        double y = lat1 - lat2;
        return Math.sqrt(x * x + y * y);
    }

    /**
     * 给定两个点，计算两点之间的速度。两点有先后顺序，因此可能返回负值
     *
     * @param p1 第一个点
     * @param p2 第二个点
     * @return 速度，m/s
     */
    public static double getSpeedInMeterPerSecond(STPoint p1, STPoint p2) {
        long timeSpanInSecond = p2.getTime().toInstant().getEpochSecond() - p1.getTime().toInstant().getEpochSecond();
        if (timeSpanInSecond == 0) {
            return 0;
        }
        double distanceInMeter = GeoFunction.getDistanceInM(p1, p2);
        return distanceInMeter / timeSpanInSecond;
    }

    /**
     * @param mbr       经纬度表示的MBR
     * @param threshold MBR扩展的宽度（M）
     * @return 扩展后的MBR
     */
    public static Envelope getExtendedMbr(Envelope mbr, double threshold) {
        double perimeter = 2 * Math.PI * EARTH_RADIUS_IN_METER;
        double latPerMeter = 360 / perimeter;
        double latBuffLen = threshold * latPerMeter;
        double minLngPerMeter = 360 / (perimeter * Math.cos(Math.toRadians(mbr.getMinY())));
        double minLngBuffLen = threshold * minLngPerMeter;
        double maxLngPerMeter = 360 / (perimeter * Math.cos(Math.toRadians(mbr.getMaxY())));
        double maxLngBuffLen = threshold * maxLngPerMeter;

        return new Envelope(mbr.getMinX() - minLngBuffLen, mbr.getMaxX() + maxLngBuffLen, mbr.getMinY() - latBuffLen, mbr.getMaxY() + latBuffLen);
    }

    /**
     * @param point     point
     * @param threshold MBR扩展的宽度（M）
     * @return 扩展后的MBR
     */
    public static Envelope getExtendedMbr(Point point, double threshold) {
        double perimeter = 2 * Math.PI * EARTH_RADIUS_IN_METER;
        double latPerMeter = 360 / perimeter;
        double latBuffLen = threshold * latPerMeter;
        double minLngPerMeter = 360 / (perimeter * Math.cos(Math.toRadians(point.getY())));
        double lngBuffLen = threshold * minLngPerMeter;
        return new Envelope(point.getX() - lngBuffLen, point.getX() + lngBuffLen, point.getY() - latBuffLen, point.getY() + latBuffLen);
    }

    /**
     * 计算两个MBR并起来后的MBR
     *
     * @param mbr1 第一个mbr
     * @param mbr2 第二个mbr
     * @return 并起来后的MBR
     */
    public static Envelope getUnionMbr(Envelope mbr1, Envelope mbr2) {
        Envelope mbr = new Envelope(mbr1);
        mbr.expandToInclude(mbr2);
        return mbr;
    }

    /**
     * 计算两个点之间的倾斜角度
     *
     * @param startPt 点
     * @param endPt   点
     * @return 倾斜角度
     */
    private static double bearing(SpatialPoint startPt, SpatialPoint endPt) {
        double ptALatRad = Math.toRadians(startPt.getLat());
        double ptALngRad = Math.toRadians(startPt.getLon());
        double ptBLatRad = Math.toRadians(endPt.getLat());
        double ptBLngRad = Math.toRadians(endPt.getLon());
        double y = Math.sin(ptBLngRad - ptALngRad) * Math.cos(ptBLatRad);
        double x = Math.cos(ptALatRad) * Math.sin(ptBLatRad) - Math.sin(ptALatRad) * Math.cos(ptBLatRad) * Math.cos(ptBLngRad - ptALngRad);
        double bearingRad = Math.atan2(y, x);
        return (Math.toDegrees(bearingRad) + 360.0) % 360.0;
    }

    /**
     * 根据rate 求出投影点的经纬度
     *
     * @param startPt 起始点
     * @param endPt   终点
     * @param rate    a到投影点距离 / ab距离
     * @return 投影点
     */
    private static SpatialPoint calLocAlongLine(SpatialPoint startPt, SpatialPoint endPt, double rate) {
        double lat = startPt.getLat() + rate * (endPt.getLat() - startPt.getLat());
        double lng = startPt.getLon() + rate * (endPt.getLon() - startPt.getLon());
        return new SpatialPoint(lng, lat);
    }

    /**
     * 将点投影到线， 如果点在 ab组成这个线段的左边， 投影为a， 如果在右边，投影为b， 如果在中间，按照rate求出对应投影点的经纬度
     *
     * @param startPt 点 一个rs上的起始点
     * @param endPt   点 一个rs上的终点
     * @param pt      待投影的点
     * @return 投影到segment上的点
     */
    public static ProjectionPoint projectPtToSegment(SpatialPoint startPt, SpatialPoint endPt, SpatialPoint pt) {
        double abAngle = bearing(startPt, endPt);
        double atAngle = bearing(startPt, pt);
        double abLength = getDistanceInM(startPt, endPt);
        double atLength = getDistanceInM(startPt, pt);
        double deltaAngle = atAngle - abAngle;
        double metersAlong = atLength * Math.cos(Math.toRadians(deltaAngle));
        double rate;
        SpatialPoint projection;
        if (abLength == 0.0) {
            rate = 0.0;
        } else {
            rate = metersAlong / abLength;
        }
        if (rate > 1.0) {
            projection = new SpatialPoint(endPt.getLon(), endPt.getLat());
        } else if (rate < 0) {
            projection = new SpatialPoint(startPt.getLon(), startPt.getLat());
        } else {
            projection = calLocAlongLine(startPt, endPt, rate);
        }
        double dist = getDistanceInM(pt, projection);
        return new ProjectionPoint(projection, dist, rate);
    }

    /**
     * 二分法快速查找映射点
     *
     * @param pt     原始点
     * @param points 映射路段的point list
     * @param start  起始index
     * @param end    结束index
     * @return 投影点, 投影点对应的point list 的index
     */
    public static Tuple2<ProjectionPoint, Integer> calProjection(SpatialPoint pt, List<SpatialPoint> points, int start, int end) {
        if (end - start == 1) {
            return new Tuple2<>(projectPtToSegment(points.get(start), points.get(end), pt), start);
        }
        int mid = (start + end) / 2;
        ProjectionPoint projectionPoint = projectPtToSegment(points.get(start), points.get(mid), pt);
        double rate = projectionPoint.getRate();
        if (rate > 1.0) {
            start = mid;
        } else {
            end = mid;
        }
        return calProjection(pt, points, start, end);
    }

    /**
     * 重载，输入geometry，将geometry的坐标转换后求面积
     *
     * @param geometry polygon
     * @return 面积 单位为平方米
     */
    public static double getPolygonArea(Geometry geometry) {
        Coordinate[] coordinates = Arrays.stream(geometry.getBoundary().getCoordinates()).map(GeoFunction::transform).toArray(Coordinate[]::new);
        return geometry.getFactory().createPolygon(coordinates).getArea();
    }

    /**
     * 坐标转换的辅助方程
     *
     * @param coordinate 坐标
     * @return 转换后的坐标
     */
    private static Coordinate transform(Coordinate coordinate) {
        double[] convertedCoord = gcsToMiller(coordinate.x, coordinate.y);
        return new Coordinate(convertedCoord[0], convertedCoord[1]);
    }

}
