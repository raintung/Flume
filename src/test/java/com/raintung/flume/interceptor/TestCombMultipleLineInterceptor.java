package com.raintung.flume.interceptor;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.junit.Assert;
import org.junit.Test;
import java.nio.charset.Charset;

public class TestCombMultipleLineInterceptor {

		  /**
		   * Ensure that the "host" header gets set (to something)
		   */
		  @Test
		  public void testBasic() throws Exception {
			Class cls = CombMultipleLineInterceptor.Builder.class;
		    Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
		    		cls.getName());
		    Context context = new Context();
		    context.put(CombMultipleLineInterceptor.Builder.REGEX, ".*test.*");
		    builder.configure(context);
		    Interceptor interceptor = builder.build();
		    Event event1 = EventBuilder.withBody("test event",
		            Charset.forName("UTF-8"));
		    Event eventA1 = interceptor.intercept(event1);
		    Assert.assertNull(eventA1);
		    Event event2 = EventBuilder.withBody("test event  2",
		            Charset.forName("UTF-8"));
		    Event eventA2 = interceptor.intercept(event2);
		    
		    Assert.assertSame(eventA2, event1);
		    StringBuilder sb = new StringBuilder();
		    sb.append("test event  3");
		    Event event3 = EventBuilder.withBody("test event  3",
		            Charset.forName("UTF-8"));
		    Event eventA3 = interceptor.intercept(event3);
		    Assert.assertSame(eventA3, event2);
		    sb.append("event  4");
		    Event event4 = EventBuilder.withBody("event  4",
		            Charset.forName("UTF-8"));
		    Event eventA4 = interceptor.intercept(event4);
		    Assert.assertNull(eventA4);
		    sb.append("event  5");
		    Event event5 = EventBuilder.withBody("event  5",
		            Charset.forName("UTF-8"));
		    Event eventA5 = interceptor.intercept(event5);
		    Assert.assertNull(eventA5);
		    Event event6 = EventBuilder.withBody("test event  6",
		            Charset.forName("UTF-8"));
		    Event eventA6 = interceptor.intercept(event6);
		    String result = new String(eventA6.getBody());
		    Assert.assertEquals(result, sb.toString());
		    Event event7 = EventBuilder.withBody("test event  7",
		            Charset.forName("UTF-8"));
		    Event eventA7 = interceptor.intercept(event7);
		    Assert.assertSame(eventA7, event6);
		  }

}
