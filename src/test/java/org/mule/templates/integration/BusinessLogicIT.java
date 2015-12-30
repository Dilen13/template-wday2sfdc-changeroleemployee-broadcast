/**
 * Mule Anypoint Template
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates.integration;

import static org.junit.Assert.assertNotNull;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.context.notification.NotificationException;
import org.mule.processor.chain.SubflowInterceptingChainLifecycleWrapper;
import org.mule.streaming.ConsumerIterator;

import com.mulesoft.module.batch.BatchTestHelper;

/**
 * The objective of this class is to validate the correct behavior of the flows
 * for this Anypoint Tempalte that make calls to external systems.
 */
public class BusinessLogicIT extends AbstractTemplateTestCase {

    private static final long TIMEOUT_MILLIS = 120000;
    private static final long DELAY_MILLIS = 500;
	private static String WORKDAY_WORKER_ID;
    private BatchTestHelper helper;
	private static String PERMISSION_ID;
    private static final String PATH_TO_TEST_PROPERTIES = "./src/test/resources/mule.test.properties";
	private static String SFDC_USER_ID;
	private Map<String, String> roleQuery = new HashMap<String, String>();
	
    @BeforeClass
    public static void beforeTestClass() {
        System.setProperty("poll.startDelayMillis", "8000");
        System.setProperty("poll.frequencyMillis", "30000");
        Date initialDate = new Date(System.currentTimeMillis() - (1000 * 60 * 60 * 24));
        Calendar cal = Calendar.getInstance();
        cal.setTime(initialDate);
        System.setProperty(
        		"watermark.default.expression", 
        		"#[groovy: new GregorianCalendar("
        				+ cal.get(Calendar.YEAR) + ","
        				+ cal.get(Calendar.MONTH) + ","
        				+ cal.get(Calendar.DAY_OF_MONTH) + ","
        				+ cal.get(Calendar.HOUR_OF_DAY) + ","
        				+ cal.get(Calendar.MINUTE) + ","
        				+ cal.get(Calendar.SECOND) + ") ]");
        
        final Properties props = new Properties();
		try {
			props.load(new FileInputStream(PATH_TO_TEST_PROPERTIES));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		WORKDAY_WORKER_ID = props.getProperty("wday.test.worker.id");
		SFDC_USER_ID = props.getProperty("sfdc.test.user.id");
		PERMISSION_ID = props.getProperty("sfdc.test.permission.id");
    }

    @SuppressWarnings(value="unchecked")
    private ConsumerIterator<Map<String, String>> findRole() throws MuleException, Exception{
    	SubflowInterceptingChainLifecycleWrapper flow = getSubFlow("queryPermissionFlow");
	    flow.initialise();	    
	    roleQuery.put("Id", SFDC_USER_ID);
	    roleQuery.put("RoleId", PERMISSION_ID);
	    MuleEvent res = flow.process(getTestEvent(roleQuery));
	    return (ConsumerIterator<Map<String, String>>) res.getMessage().getPayload();	    
    }
    
    @Before
    public void setUp() throws Exception {
        stopFlowSchedulers(POLL_FLOW_NAME);
        prepareWdayData();
        helper = new BatchTestHelper(muleContext);
        registerListeners();
        ConsumerIterator<Map<String, String>> iter = findRole();
	    if (iter.hasNext()){
		    Map<String, String> perm = new HashMap<String, String>();
		    Map<String, String> perm1 = iter.next();
		    perm.put("Id", perm1.get("Id"));
		    SubflowInterceptingChainLifecycleWrapper flow = getSubFlow("deletePermissionFlow");
		    flow.initialise();
		    flow.process(getTestEvent(perm));
	    }
    } 

    private void prepareWdayData() throws MuleException, Exception {    	
    	Map<String, Object> user = new HashMap<String, Object>();
    	Random r = new Random();
    	user.put("date", "2000" + "/" + (r.nextInt(12) + 1) + "/" + (r.nextInt(28) + 1));    	
    	user.put("id", WORKDAY_WORKER_ID);    	
    	logger.info("Updating workday worker: " + user);
		SubflowInterceptingChainLifecycleWrapper flow = getSubFlow("updateWorkdayFlow");
		flow.initialise();
		flow.process(getTestEvent(user));
	}
	
	private void registerListeners() throws NotificationException {
        muleContext.registerListener(pipelineListener);
    }

    @Test
    public void testMainFlow() throws Exception {
    	
        runSchedulersOnce(POLL_FLOW_NAME);
        waitForPollToRun();
        helper.awaitJobTermination(TIMEOUT_MILLIS, DELAY_MILLIS);
        helper.assertJobWasSuccessful();
        
        ConsumerIterator<Map<String, String>> iter = findRole();
        Map<String, String> perm = iter.next();
        assertNotNull("Role should be synced", perm);
        logger.info("role: " + perm);
    }
}
