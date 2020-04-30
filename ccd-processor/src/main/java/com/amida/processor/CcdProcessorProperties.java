package com.amida.processor;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("test")
public class CcdProcessorProperties {

	private static final String DEFAULT_EXPRESSION = "payload";

	private String expression = DEFAULT_EXPRESSION;

	public void setExpression(String expression) {
		this.expression = expression;
	}

	public String getExpression() {
		return expression;
	}

}