package net.paulboocock.core;

import cats.data.Validated;
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event;
import com.snowplowanalytics.snowplow.analytics.scalasdk.ParsingError;

public class App {

    private static String record = "{{MY-SITE-ID}}\tweb\t2020-10-23 15:59:59.197\t2020-10-23 15:59:58.994\t2020-10-23 15:59:58.979\tpage_view\t3b16e419-7f5b-4c12-9deb-02f5c455155f\t\tsp\tjs-2.16.2\tssc-1.0.0-kafka\tstream-enrich-1.4.0-common-1.4.0\t\t192.168.0.184\t\tfa5fe7d1-7276-4a5d-b1d5-65eecebe2721\t1\te01600c1-d336-474b-8968-048a0d5dacd5\t\t\t\t\t\t\t\t\t\t\t\thttp://my-site/\tmy-site-page-heading\thttp://my-site/\thttp\tmy-site\t80\t/\t\t\thttp\tmy-site\t80\t/\t\t\t\t\t\t\t\t\t\t\t{\"schema\":\"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0\",\"data\":[{\"schema\":\"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0\",\"data\":{\"id\":\"58548b84-318f-4c39-abfc-731440c5acd3\"}}]}\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tMozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:82.0) Gecko/20100101 Firefox/82.0\t\t\t\t\t\ten-US\t\t\t\t\t\t\t\t\t\t1\t24\t1853\t974\t\t\t\tmy-tz\t\t\t1920\t1080\tUTF-8\t1853\t974\t\t\t\t\t\t\t\t\t\t\t\t2020-10-23 15:59:58.981\t\t\t\t2f878cfe-ccc6-4e93-a3fc-2bac0fd12857\t2020-10-23 15:59:58.992\tcom.snowplowanalytics.snowplow\tpage_view\tjsonschema\t1-0-0\t\t";
    
	public static void main(String[] args) {

        Validated<ParsingError, Event> validatedEvent = Event.parse(record);
        if (validatedEvent.isValid()) {
            Event ev = validatedEvent.toOption().get();
            String jsonString = ev.toJson(false).toString(); // Or true if you want lossy output

            System.out.println(ev.platform().get());
            System.out.println(ev.collector_tstamp().getEpochSecond());
            System.out.println(ev.event().get());

            System.out.println(jsonString);

        } else {
            System.out.println("Unable to parse");
        }
    }

}
