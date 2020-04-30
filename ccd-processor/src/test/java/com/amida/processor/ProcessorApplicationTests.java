package com.amida.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.Message;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.integration.support.MessageBuilder;


@SpringBootTest
class ProcessorApplicationTests {

    @Autowired
    private Processor processor;

    @Autowired
    private MessageCollector messageCollector;

	/**
	 * Testing to see that the spring boot app loads at al
	 */
	@Test
	public void contextLoads() {
	}

	/**
	 * Testing to confirm that the processor can read a byte array that is a CCD and outputs FHIR json
	 * @throws Exception
	 */
	@Test
	public void testCcdProcessor() throws Exception {
		//Sample CCD
		InputStream is = this.getClass().getResourceAsStream("/C-CDA_R2-1_CCD.xml");
		//Read CCD into byte array
		byte[] payload = new byte[is.available()];
		is.read(payload);

		//Adding a header to confirm that headers are preserved by the processor
		Map<String, String> headers = new HashMap<>();
		headers.put("testkey", "testValue");
		//Create the message to send with a payload of the CCD and the test headers
		Message < byte[] > m = MessageBuilder
			.withPayload(payload)
			.copyHeaders(headers)
			.build();
		//Send the message
		this.processor.input().send(m);
		//Wait until the message is returned
		//NOTE: We only wait 1 second, for large CCDs ot slow systems this might not be enough?  We should profile the CCD parser code
		Message < ? > message = this.messageCollector.forChannel(this.processor.output()).poll(1, TimeUnit.SECONDS);
		//Get the response payload
		String out = message.getPayload().toString();
		Map<String, ?> respHeaders = message.getHeaders();
		
		//confirm we got back json that is a bundle
		assertTrue(out.indexOf("\"resourceType\": \"Bundle\"") >0);
		//confirm original headers were preserved
		assertTrue(headers.get("testkey").equals(respHeaders.get("testkey")));
	}

		/**
	 * Testing to confirm that the processor can read a byte array that is a CCD and outputs FHIR json
	 * @throws Exception
	 */
	@Test
	public void testCcdProcessorError() throws Exception {

		//Create an invalid CCD
		byte[] payload = "This is not a valid CCD".getBytes();
	
		//Adding a header to confirm that headers are preserved by the processor
		Map<String, String> headers = new HashMap<>();
		headers.put("testkey", "testValue");
		//Create the message to send with a payload of the CCD and the test headers
		Message < byte[] > m = MessageBuilder
			.withPayload(payload)
			.copyHeaders(headers)
			.build();
		//Send the message
		this.processor.input().send(m);
		//Wait until the message is returned
		//NOTE: We only wait 1 second, for large CCDs ot slow systems this might not be enough?  We should profile the CCD parser code
		Message < ? > message = this.messageCollector.forChannel(this.processor.output()).poll(1, TimeUnit.SECONDS);
		//Get the response payload
		String out = message.getPayload().toString();
		Map<String, ?> respHeaders = message.getHeaders();
		
		//confirm we got back an empty json since there should be an error
		assertEquals("{}", out);
		// //Confirm the error header is present
		assertTrue(respHeaders.get("error").equals("true"));
		assertTrue(respHeaders.get("errorMessage").equals("Content is not allowed in prolog."));
		//confirm original headers were preserved
		assertTrue(headers.get("testkey").equals(respHeaders.get("testkey")));
	}
}
