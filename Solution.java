package com.example.Solution;
/*
 * Click `Run` to execute the snippet below!
 */

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

/*
 * To execute Java, please define "static void main" on a class
 * named Solution.
 *
 * If you need more classes, simply define them inline.
 */
class Flight
{
    String icao24;
    long firstSeen;
    String estDepartureAirport;
    long lastSeen;
    String estArrivalAirport;
    long flightDuration;

    public String getIcao24() {
        return icao24;
    }

    public void setIcao24(String icao24) {
        this.icao24 = icao24;
    }


    public long getFirstSeen() {
        return firstSeen;
    }

    public void setFirstSeen(long firstSeen) {
        this.firstSeen = firstSeen;
    }

    public String getEstDepartureAirport() {
        return estDepartureAirport;
    }

    public void setEstDepartureAirport(String estDepartureAirport) {
        this.estDepartureAirport = estDepartureAirport;
    }

    public long getLastSeen() {
        return lastSeen;
    }

    public void setLastSeen(long lastSeen) {
        this.lastSeen = lastSeen;
    }

    public String getEstArrivalAirport() {
        return estArrivalAirport;
    }

    public void setEstArrivalAirport(String estArrivalAirport) {
        this.estArrivalAirport = estArrivalAirport;
    }

    public long getFlightDuration() {
        return flightDuration;
    }

    public void setFlightDuration(long flightDuration) {
        this.flightDuration = flightDuration;
    }

}
class RestClient
{
    private static final String ROOT_URL = "https://opensky-network.org";
    private String requestMethod;
    private String endPoint;
    private String queryParams;

    //Default Constructor
    public RestClient()
    {
        this.endPoint="";
        this.queryParams="";
    }
    public RestClient(String endPoint)
    {
        this.endPoint=endPoint;
        this.queryParams="";
    }

    public RestClient(String arrival_endpoint, String arrival_airport, long currentUnixTime, long unixTimeStampOf_n_daysBefore) {
        this.endPoint=arrival_endpoint;
        this.queryParams="?airport="+arrival_airport+"&begin="+String.valueOf(unixTimeStampOf_n_daysBefore)+"&end="+String.valueOf(currentUnixTime);
    }

    public String getRequestMethod() {
        return requestMethod;
    }

    public void setRequestMethod(String requestMethod) {
        this.requestMethod = requestMethod;
    }
    public JSONArray sendRequest() throws Exception
    {
        JSONArray json=null;
        try{
            URL url = new URL(ROOT_URL+endPoint+queryParams);
            HttpURLConnection httpURLConnection= (HttpURLConnection) url.openConnection();
            httpURLConnection.setRequestMethod(getRequestMethod());
            if (httpURLConnection.getResponseCode() != 200) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + httpURLConnection.getResponseCode());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (httpURLConnection.getInputStream())));

            String output;
            System.out.println("Output from Server .... \n");

            JSONParser parser = new JSONParser();
            while ((output = br.readLine()) != null) {
                json = (JSONArray) parser.parse(output);
            }

            httpURLConnection.disconnect();

        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
        finally {

        }
        return json;
    }
}
class FlightDuration implements Comparator<Map.Entry<String, Long>> {

    @Override
    public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
        if(o1.getValue()>o2.getValue())
            return -1;
        else if(o1.getValue()<o2.getValue())
            return 1;
        else
            return 0;
    }
}
class FlightDataProcessor
{

    public Map<String, List<Flight>> categorizeFlightsByDepartureAirport(JSONArray json) {
        Map<String, List<Flight>> departureAirportToListOfFlights = new HashMap<>();
        for(int i=0;i<json.size();i++)
        {

            JSONObject obj = (JSONObject) json.get(i);
            Flight flight=new Flight();
            flight.setIcao24((String) obj.get("icao24"));
            flight.setFirstSeen((long) obj.get("firstSeen"));
            flight.setLastSeen((long) obj.get("lastSeen"));
            flight.setEstDepartureAirport((String) obj.get("estDepartureAirport"));
            flight.setEstArrivalAirport((String) obj.get("estArrivalAirport"));
            flight.setFlightDuration((long) obj.get("lastSeen")-(long) obj.get("firstSeen"));
            List<Flight> flights;
            if(departureAirportToListOfFlights.containsKey(flight.getEstDepartureAirport()))
                flights = departureAirportToListOfFlights.get(flight.getEstDepartureAirport());
            else
                flights = new ArrayList<>();

            if(flights==null)
                throw new NullPointerException();
            flights.add(flight);
            departureAirportToListOfFlights.put(flight.getEstDepartureAirport(),flights);
        }
        return departureAirportToListOfFlights;
    }

    public Map<String, Long> findMostVariedFlightDuration(Map<String, List<Flight>> departureAirportToListOfFlights) {
        Map<String, Long> departureAirportToMostVariedFlightDuration = new HashMap<>();

        for (Map.Entry<String,List<Flight>> entry : departureAirportToListOfFlights.entrySet())
        {
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            List<Flight> flights = entry.getValue();
            for(Flight flight:flights)
            {
                if(flight.getFlightDuration()<min)
                    min = flight.getFlightDuration();
                if(flight.getFlightDuration()>max)
                    max = flight.getFlightDuration();
            }
            departureAirportToMostVariedFlightDuration.put(entry.getKey(),max-min);
        }
        return sortMapByValue(departureAirportToMostVariedFlightDuration);
    }

    private Map<String, Long> sortMapByValue(Map<String, Long> departureAirportToMostVariedFlightDuration) {
        List<Map.Entry<String, Long>> listOfMap
                = new ArrayList<Map.Entry<String, Long> >(
                departureAirportToMostVariedFlightDuration.entrySet());

        Collections.sort(listOfMap, new FlightDuration());
        Map<String, Long> sortedMap = new LinkedHashMap<String, Long>();
        for (Map.Entry<String, Long> entry : listOfMap) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }
}
public class Solution {
    public static void main(String[] args) throws Exception {

        final String ARRIVAL_ENDPOINT = "/api/flights/arrival";
        final String ARRIVAL_AIRPORT = "KSFO";
        final int NUMBER_OF_DAYS = 3; //It cannot be more than 7
        long currentUnixTime = System.currentTimeMillis() / 1000L;
        long unixTimeStampOf_N_DaysBefore = currentUnixTime - (long)NUMBER_OF_DAYS*24*60*60; //current time - seconds in N days
        RestClient restClient = new RestClient(ARRIVAL_ENDPOINT,ARRIVAL_AIRPORT,currentUnixTime,unixTimeStampOf_N_DaysBefore);
        restClient.setRequestMethod("GET");
        JSONArray json = restClient.sendRequest();
        if(json==null)
            throw new NullPointerException("Null Response");
        FlightDataProcessor flightDataProcessor=new FlightDataProcessor();

        Map<String, List<Flight>> departureAirportToListOfFlights = flightDataProcessor.categorizeFlightsByDepartureAirport(json);

        Map<String, Long> departureAirportToMostVariedFlightDuration = flightDataProcessor.findMostVariedFlightDuration(departureAirportToListOfFlights);

        System.out.println(departureAirportToMostVariedFlightDuration);
        int count = 0;

        Iterator mapIterator = departureAirportToMostVariedFlightDuration.entrySet().iterator();

        Date startDateTime = new Date(unixTimeStampOf_N_DaysBefore*1000); //argument should be in milliseconds
        Date endDate = new Date(currentUnixTime*1000); //argument should be in milliseconds
        System.out.println("----------------------------------------------------------------------------");
        System.out.println("Interval\t"+ startDateTime +", "+endDate);
        System.out.println("----------------------------------------------------------------------------");
        System.out.println("Airport Code | (max - min flight duration)");
        System.out.println("----------------------------------------------------------------------------");

        while(mapIterator.hasNext() && count<5) // We are interested in top 5 varied airports so limiting the iteration to 5 but map is sorted, so it can be changed
        {
            Map.Entry mapElement = (Map.Entry)mapIterator.next();
            if(mapElement.getKey()!=null) //Departure Airport for some flights are unknown, so null keys(departure airport) can be skipped
            {
                long duration = (long) mapElement.getValue();
                System.out.print("\t"+mapElement.getKey());
                System.out.print("\t|");

                String formattedDuration= ":"+String.valueOf(duration%60L); //getting the remaining seconds
                duration = duration / 60L; //converting into minutes;

                formattedDuration= ":"+String.valueOf(duration%60L)+formattedDuration; //getting the remaining minutes
                duration = duration / 60L; //converting into hours;

                formattedDuration= String.valueOf(duration)+formattedDuration;
                System.out.println("\t"+formattedDuration);
                count++;
            }
        }
    }
}

/**
 This is just a simple shared plaintext pad, with no execution capabilities.

 When you know what language you'd like to use for your interview,
 simply choose it from the dropdown in the top bar.

 You can also change the default language your pads are created with
 in your account settings: https://app.coderpad.io/settings

 Enjoy your interview!

 ===== Preface =====

 This question is very difficult in C and C++, where there is
 insufficient library support to answer it in an hour. If you
 prefer to program in one of those languages, please ask us to
 provide you with a question designed for those languages instead!


 ===== Intro =====

 San Francisco's International Airport (KSFO) continuously tries
 to improve the quality of its flight estimates by analyzing data
 from past flights and figuring out the root-cause of outliers
 and inaccurate estimates. In order to do this the teams that are
 tasked to solve this problem need to figure out ways of finding
 those outliers in their data.

 For the purpose of this interview question, imagine that you are
 one of the engineers working on this problem. One of your colleagues
 suggested during a meeting that a good first step on finding those
 outliers would be categorize past flight information by departure
 airport (e.g. the airport that a plane departs from to arrive at KSFO)
 and focus on the ones whose flight duration varies the most. Your team
 decided that this idea is worth pursuing and you're tasked with
 implementing it.

 Your goal is to write a small program that utilizes the OpenSky
 public API and prints the 5 most varied flight duration data
 from flights that arrived in KSFO the past 3 days, categorized by
 departure airport.

 Here is an example of the output of such a utility:

 ---------------------------------------------------
 Interval [2021-03-08 18:03:24, 2021-03-11 18:03:24]
 ---------------------------------------------------
 Airport Code | (max - min flight duration)
 ---------------------------------------------------
 LFPG         | 23:27:18
 KORD         | 5:20:06
 KDEN         | 4:16:37
 VIDP         | 1:03:25
 KEWR         | 0:44:26


 Interpreting the above data we can see that in the past 3 days
 we saw a big outlier in which the slowest flight from LFPG
 (Charles de Gaulle in Paris) was more than 23 hours slower than
 the quickest flight from that same airport.

 Rules/constraints:
 * Print the 5 departure airports in sorted order (the airport
 with the maximum flight difference should appear at the top
 and the rest shall follow in descending order).
 * Limit the output to 5 departure airports
 * Print actual difference in time between the longest and the
 shortest flight duration.

 Your output does not need to match the above example, The example
 mostly exists to explain the problem and act as a guide to what
 what we are looking for. If you have better ideas of how to display
 the data, please do!

 You should implement this in whatever language you're most
 comfortable with -- just make sure your code is production
 quality, well designed, and easy to read.

 Finally, please help us by keeping this question and your
 answer secret so that every candidate has a fair chance in
 future Delphix interviews.


 ===== Steps =====

 1.  Choose the language you want to code in from the menu
 labeled "Plain Text" in the top right corner of the
 screen. You will see a "Run" button appear on the top
 left -- clicking this will send your code to a Linux
 server and compile / run it. Output will appear on the
 right side of the screen.

 For information about what libraries are available for
 your chosen language, see:

 https://coderpad.io/languages

 2.  Pull up the documentation for the API you'll be using:

 https://openskynetwork.github.io/opensky-api/rest.html

 3. The API has most of its examples using authentication
 credentials like so:

 https://USERNAME:PASSWORD@opensky-network.org/api/flights/arrival?airport=KSFO..etc..

 You don’t need actual credentials to use the API. For
 the same example request show above just remove the
 “USERNAME:PASSWORD@” part like so:

 https://opensky-network.org/api/flights/arrival?airport=KSFO..etc...

 4.  Implement the functionality described above, using data
 fetched dynamically from the Arrivals-By-Airport API
 described here:

 https://openskynetwork.github.io/opensky-api/rest.html#arrivals-by-airport

 5.  Output any results through the main() method of
 your program so that we can easily run them.


 ====== FAQs =====

 Q:  How do I know if my solution is correct?
 A:  Make sure you've read the prompt carefully and you're
 convinced your program does what you think it should
 in the common case. If your program does what the prompt
 dictates, you will get full credit. We do not use an
 auto-grader, so we do not have any values for you to
 check correctness against.

 Q:  What is Delphix looking for in a solution?
 A:  After submitting your code, we'll have a pair of engineers
 evaluate it and determine next steps in the interview process.
 We are looking for correct, easy-to-read, robust code.
 Specifically, ensure your code is idiomatic and laid out
 logically. Ensure it is correct. Ensure it handles all edge
 cases and error cases elegantly.

 Q:  If I need a clarification, who should I ask?
 A:  Send all questions to the email address that sent you
 this document, and an engineer at Delphix will get
 back to you ASAP (we're pretty quick during normal
 business hours).

 Q:  How long should this question take me?
 A:  Approximately 1 hour, but it could take more or less
 depending on your experience with web APIs and the
 language you choose.

 Q:  When is this due?
 A:  We will begin grading your answer 24 hours after it is
 sent to you, so that is the deadline.

 Q:  What if something comes up and I cannot complete the
 problem during the 24 hours?
 A:  Reach out to us and let us know! We will work with you
 to figure out an extension if necessary.

 Q:  How do I turn in my solution?
 A:  Anything you've typed into this document will be saved.
 If you were given a Takehome question, there should be a Submit
 Button in the bottom right of the coderpad page. If you do not
 see such a button, feel free to email us when you are done with
 your solution. We will respond confirming we've received the
 solution within 24 hours.

 Q:  Can I use any external resources to help me?
 A:  Absolutely! Feel free to use any online resources you
 like, but please don't collaborate with anyone else.

 Q:  Can I use my favorite library in my program?
 A:  Unfortunately, there is no way to load external
 libraries into CoderPad, so you must stick to what
 they provide out of the box for your language:

 https://coderpad.io/languages

 If you really want to use something that's not
 available, email the address that sent you this link
 and we will work with you to find a solution.

 Q:  Can I code this up in a different IDE?
 A:  Of course! However, we do not have your environment
 to run your code in. We ask that you submit your final
 code via CoderPad (and make sure it can run). This gives
 our graders the ability to run your code rather than guessing.

 Q:  Why does my program terminate unexpectedly in
 CoderPad, and why can't I read from stdin or pass
 arguments on the command line?
 A:  CoderPad places a limit on the runtime and amount of
 output your code can use, but you should be able to
 make your code fit within those limits. You can hard
 code any arguments or inputs to the program in your
 main() method or in your tests.

 Q:  I'm a Vim/Emacs fan -- is there any way to use those
 keybindings? What about changing the tab width? Font
 size?
 A:  Yes! Hit the button at the bottom of the screen that
 looks like a keyboard.
 */

