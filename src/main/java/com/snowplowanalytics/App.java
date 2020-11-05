package com.snowplowanalytics;

import cats.data.Validated;
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event;
import com.snowplowanalytics.snowplow.analytics.scalasdk.ParsingError;

public class App {

    private static final String record = "{{MY-SITE-ID}}\tweb\t2020-10-26 15:00:00.015\t2020-10-26 15:00:00.001\t2020-10-26 14:59:59.059\tpage_view\tafc17d66-cdd6-4f0b-a3c4-97521af63825\t\tsp\tjs-2.16.2\tssc-1.0.0-kafka\tstream-enrich-1.4.0-common-1.4.0\t\t192.168.0.2\t\t4c0ce0ea-66f6-4144-96f8-1155550ea2a5\t1\t136df257-4267-4af1-8338-552f95e5b352\t\t\t\t\t\t\t\t\t\t\t\thttps://snowplowanalytics.com/\tSnowplow Analytics\thttps://snowplowanalytics.com/\thttps\tsnowplowanalytics.com\t80\t/\t\t\thttps\tsnowplowanalytics.com\t80\t/\t\t\t\t\t\t\t\t\t\t\t{\"schema\":\"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0\",\"data\":[{\"schema\":\"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0\",\"data\":{\"id\":\"ff5fa637-3c07-42d9-9516-2aea4b15c60c\"}}]}\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tMozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:81.0) Gecko/20100101 Firefox/81.0\t\t\t\t\t\ten-US\t\t\t\t\t\t\t\t\t\t1\t24\t1853\t974\t\t\t\tmy-tz\t\t\t1920\t1080\tUTF-8\t1853\t974\t\t\t\t\t\t\t\t\t\t\t\t2020-10-26 15:00:00.000\t\t\t\t538339ca-3900-444d-af20-3b01e219ad01\t2020-10-26 15:00:00.010\tcom.snowplowanalytics.snowplow\tpage_view\tjsonschema\t1-0-0\t\t";
  
	public static void main(String[] args) {

        // Convert Enriched TSV String using Scala Analytics SDK
        Validated<ParsingError, Event> validatedEvent = Event.parse(record);

        // Check if String has been parsed correctly
        if (validatedEvent.isValid()) {

            // Get Event object 
            Event ev = validatedEvent.toOption().get();

            // Convert to JSON String if desired
            String jsonString = ev.toJson(false).noSpaces(); // Or true if you want lossy output

            // Get individual properties of the event
            System.out.println(ev.platform().get());
            System.out.println(ev.collector_tstamp().getEpochSecond());
            System.out.println(ev.event().get());

            System.out.println(jsonString);

        } else {
            System.out.println("Unable to parse");
        }
    }

}
