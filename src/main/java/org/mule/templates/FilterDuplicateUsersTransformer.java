/**
 * Mule Anypoint Template
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.RandomStringUtils;
import org.mule.api.MuleMessage;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.AbstractMessageTransformer;
 
public class FilterDuplicateUsersTransformer extends AbstractMessageTransformer{
	@SuppressWarnings("unchecked")
    @Override
    public Object transformMessage(MuleMessage message, String outputEncoding) throws TransformerException {
        List<Map<String, String>> payload = (List<Map<String, String>>) message.getPayload();
        List<String> emails = new ArrayList<String>();

        Iterator<Map<String, String>> iterator = payload.iterator();

        while (iterator.hasNext()) {
            Map<String, String> next = iterator.next();
            String email = next.get("Email");

            if (emails.contains(email)) {
                iterator.remove();
            } else {
                emails.add(email);
                next.put("Username", RandomStringUtils.randomAlphabetic(8) + "@sf.com");
            }
        }

        return payload;
    }
}
