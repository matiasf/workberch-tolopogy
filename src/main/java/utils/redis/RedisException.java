package main.java.utils.redis;

import java.io.IOException;

public class RedisException extends IOException {

	private static final long serialVersionUID = 4162490646409782695L;
	
	public RedisException(final Exception e) {
		super(e);
	}

}
