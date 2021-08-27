package minsait.ttaa.datio.common;

import minsait.ttaa.datio.common.naming.Params;

public final class Common {
    public static final String SPARK_MODE = "local[*]";
    public static final String HEADER = "header";
    public static final String INFER_SCHEMA = "inferSchema";
    private static Params paramas = new Params();
    public static final String INPUT_PATH = paramas.getNameCsv();
    public static final String OUTPUT_PATH = paramas.getOutputFile();
    public static final Integer PARAMETER = paramas.getParameter();

}
