package com.raintung.flume.interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Combine the multiple lines if not match the regex pattern. It will hold the
 * message sending until the next match the regex pattern message coming.
 * 
 * @author keli
 *
 */
public class CombMultipleLineInterceptor implements Interceptor {
	private static final Logger logger = LoggerFactory.getLogger(CombMultipleLineInterceptor.class);

	private Pattern pattern = Pattern.compile("a*b");

	public CombMultipleLineInterceptor(Pattern pattern) {
		this.pattern = pattern;
	}

	@Override
	public void initialize() {
		// NOT to do it.
	}

	private Event preEvent = null;

	@Override
	public Event intercept(Event event) {
		String body = new String(event.getBody());
		if (!pattern.matcher(body).matches()) {
			byte[] oldbyte = preEvent.getBody();
			byte[] newbyte = event.getBody();
			byte[] combined = new byte[oldbyte.length + newbyte.length];
			System.arraycopy(oldbyte, 0, combined, 0, oldbyte.length);
			System.arraycopy(newbyte, 0, combined, oldbyte.length, newbyte.length);
			preEvent.setBody(combined);
		} else {
			if (preEvent != null) {
				Event cache = preEvent;
				preEvent = event;
				return cache;
			} else {
				preEvent = event;
			}
		}
		return null;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> newEvents = new ArrayList<Event>();
		for (Event event : events) {
			Event newEvent = intercept(event);
			if (newEvent != null) {
				newEvents.add(newEvent);
			}
		}
		return newEvents;
	}

	@Override
	public void close() {
		// NOT to do it.
	}

	/**
	 * Builder which builds new instance of the CombMutipleLineInterceptor.
	 */
	public static class Builder implements Interceptor.Builder {
		public static final String REGEX = "regex";
		private Pattern regex;

		@Override
		public void configure(Context context) {
			String regexString = context.getString(REGEX, null);
			if (regexString != null) {
				regex = Pattern.compile(regexString);
			}
		}

		@Override
		public Interceptor build() {
			if (regex == null) {
				throw new FlumeException(String.format("Creating CombMutipleLineInterceptor: regex=%s", regex));
			}
			logger.info(String.format("Creating CombMutipleLineInterceptor: regex=%s", regex));
			return new CombMultipleLineInterceptor(regex);
		}
	}
}
