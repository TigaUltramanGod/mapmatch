package mapmatch.shortestpath;

import scala.Serializable;

/**
 * @description :出行方式的enum
 * @date : Created in 2019-12-03
 * @modified by :
 **/
public enum ShortestPathAlgoTypeEnum implements Serializable {

    ASTAR(AlgorithmTypeConstant.ASTAR),

    DIJKSTRA(AlgorithmTypeConstant.DIJKSTRA),

    BI_DIJKSTRA(AlgorithmTypeConstant.BI_DIJKSTRA),

    BI_ASTAR(AlgorithmTypeConstant.BI_ASTAR),

    CH(AlgorithmTypeConstant.CH);

    String type;

    /**
     * TravelTypeEnum的构造函数
     */
    ShortestPathAlgoTypeEnum(String type) {
        this.type = type;
    }

    public static class AlgorithmTypeConstant {
        /**
         * A* 算法
         */
        static final String ASTAR = "ASTAR";

        /**
         * Dijkstra 算法
         */
        static final String DIJKSTRA = "DIJKSTRA";

        /**
         * Contraction Hierarchies 算法
         */
        static final String CH = "CH";

        static final String BI_DIJKSTRA = "BI_DIJKSTRA";

        static final String BI_ASTAR = "BI_ASTAR";
    }
}

