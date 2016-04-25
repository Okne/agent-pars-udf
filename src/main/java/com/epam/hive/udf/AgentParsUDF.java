package com.epam.hive.udf;

import java.util.ArrayList;
import java.util.List;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * User-defined function to convert range of date [startDate, endDate]
 * to array of all dates between
 */
public final class AgentParsUDF extends GenericUDF
{
    public static final String UA_TYPE_FIELD_NAME = "ua_type";
    public static final String UA_FAMILY_FIELD_NAME = "ua_family";
    public static final String OS_NAME_FIELD_NAME = "os_name";
    public static final String DEVICE_FIELD_NAME = "device";
    public static final String UNKNOWN_FIELD_VALUE = "unknown";

    private List<String> fnames = new ArrayList<>();
    private StringObjectInspector argOI;
    private transient Object[] ret;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if(objectInspectors.length != 1) {
            throw new UDFArgumentException("Only one param is required");
        }

        if (!(objectInspectors[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentException("Param should be of type STRING");
        }

        fnames.add(UA_TYPE_FIELD_NAME);
        fnames.add(UA_FAMILY_FIELD_NAME);
        fnames.add(OS_NAME_FIELD_NAME);
        fnames.add(DEVICE_FIELD_NAME);

        ret = new Object[fnames.size()];

        this.argOI = (StringObjectInspector) objectInspectors[0];

        ArrayList<ObjectInspector> retOIs = new ArrayList<>();
        for (int i = 0; i < fnames.size(); i++) {
            retOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fnames, retOIs);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String userAgentStr = argOI.getPrimitiveJavaObject(deferredObjects[0].get());
        UserAgent ua = UserAgent.parseUserAgentString(userAgentStr);

        String fieldValue;
        for (int i = 0; i < fnames.size(); i++) {
            switch(i) {
            case 0:
                fieldValue = ua.getBrowser().getBrowserType().getName();
                break;
            case 1:
                fieldValue = ua.getBrowser().getGroup().getName();
                break;
            case 2:
                fieldValue = ua.getOperatingSystem().getName();
                break;
            case 3:
                fieldValue = ua.getOperatingSystem().getDeviceType().getName();
                break;
            default:
                fieldValue = UNKNOWN_FIELD_VALUE;
            }
            ret[i] = new Text(fieldValue);
        }

        return ret;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "Convert range of date [startDate, endDate] to array of all dates between including borders";
    }
}
