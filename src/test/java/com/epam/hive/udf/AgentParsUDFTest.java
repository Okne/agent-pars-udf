package com.epam.hive.udf;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.BrowserType;
import eu.bitwalker.useragentutils.DeviceType;
import eu.bitwalker.useragentutils.OperatingSystem;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

/**
 * Unit test for AgentParsUDF class.
 */
public class AgentParsUDFTest
{
    public static final String USER_AGENT_IE_WINXP = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727)";
    public static final String USER_AGENT_STR_2 = "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1";

    public static final StringObjectInspector STRING_OBJ_INSPECTOR = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    public AgentParsUDF testObj;

    @Before
    public void setup() throws Exception {
        testObj = new AgentParsUDF();
        testObj.initialize(new ObjectInspector[] {STRING_OBJ_INSPECTOR});
    }

    @Test
    public void evaluate_HappyDayIE_test() throws Exception {
        //given
        DeferredObject userAgentDefObj = new DeferredJavaObject(USER_AGENT_IE_WINXP);

        //when
        Object[] ret = (Object[]) testObj.evaluate(new DeferredObject[] {userAgentDefObj});

        //then
        assertEquals(ret.length, 4);
        assertTrue(ret[0] instanceof Text);
        assertEquals(BrowserType.WEB_BROWSER.getName(), ret[0].toString());
        assertTrue(ret[1] instanceof Text);
        assertEquals(Browser.IE.getName(), ret[1].toString());
        assertTrue(ret[2] instanceof Text);
        assertEquals(OperatingSystem.WINDOWS_XP.getName(), ret[2].toString());
        assertTrue(ret[3] instanceof Text);
        assertEquals(DeviceType.COMPUTER.getName(), ret[3].toString());
    }

    @Test
    public void evaluate_HappyDayChrome_test() throws Exception {
        //given
        DeferredObject userAgentDefObj = new DeferredJavaObject(USER_AGENT_STR_2);

        //when
        Object[] ret = (Object[]) testObj.evaluate(new DeferredObject[] {userAgentDefObj});

        //then
        assertEquals(ret.length, 4);
        assertTrue(ret[0] instanceof Text);
        assertEquals(BrowserType.WEB_BROWSER.getName(), ret[0].toString());
        assertTrue(ret[1] instanceof Text);
        assertEquals(Browser.CHROME.getName(), ret[1].toString());
        assertTrue(ret[2] instanceof Text);
        assertEquals(OperatingSystem.WINDOWS_XP.getName(), ret[2].toString());
        assertTrue(ret[3] instanceof Text);
        assertEquals(DeviceType.COMPUTER.getName(), ret[3].toString());
    }

}
