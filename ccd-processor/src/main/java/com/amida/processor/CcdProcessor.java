package com.amida.processor;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.openhealthtools.mdht.uml.cda.consol.ConsolPackage;
import org.openhealthtools.mdht.uml.cda.consol.ContinuityOfCareDocument;
import org.openhealthtools.mdht.uml.cda.util.CDAUtil;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.SendTo;

import lombok.extern.slf4j.Slf4j;
import tr.com.srdc.cda2fhir.transform.CCDTransformerImpl;
import tr.com.srdc.cda2fhir.util.FHIRUtil;
import tr.com.srdc.cda2fhir.util.IdGeneratorEnum;

@Slf4j
@EnableBinding(Processor.class)
@EnableConfigurationProperties(CcdProcessorProperties.class)
public class CcdProcessor {

    @StreamListener(Processor.INPUT)
	@SendTo(Processor.OUTPUT)
	public Message <String> messenger(Message < byte[] > message) {
        
        //get payload from message
		byte[] payload = message.getPayload();
        
        //XXX: should we execute the loadPackages in a static initializer?
        CDAUtil.loadPackages();

        try{
            //put the payload into an input stream
            ByteArrayInputStream is = new ByteArrayInputStream(payload);
            //load the CCD
            ContinuityOfCareDocument cda = (ContinuityOfCareDocument) CDAUtil.loadAs(is,
                    ConsolPackage.eINSTANCE.getContinuityOfCareDocument());
    
            CCDTransformerImpl ccdTransformer = new CCDTransformerImpl(IdGeneratorEnum.COUNTER);
            //convert to FHIR bundle
            Bundle bundle = ccdTransformer.transformDocument(cda, BundleType.TRANSACTION, null, null, null);
            //output the FHIR bundle to a String
            String bundleString = FHIRUtil.encodeToJSON(bundle);
            //return the resulting FHIR string
            Message < String > m = MessageBuilder
			.withPayload(bundleString)
            .build();
            return m;
        }catch(Exception e){
            //If an error return an empty JSON object and add to the headers
            //the error message
            log.error("exception in CCD processing", e);      
            Map<String, String> extra = new HashMap<>();
            extra.put("error", "true");
            extra.put("errorMessage", e.getMessage());
            Message < String > m = MessageBuilder
                .withPayload("{}")
                .copyHeaders(extra)
                .build();
            return m;
        }






	}

}