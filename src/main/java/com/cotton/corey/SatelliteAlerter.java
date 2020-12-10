package com.cotton.corey;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.DateTimeException;
import java.util.Map;
import java.util.HashMap;
import java.time.LocalDateTime;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;
import javafx.util.Pair;

public class SatelliteAlerter {

    //All constants could be externalized to a properties file or they could be loaded into a singleton for use as a global immutable reference.
    private static byte YEAR_POSITION = 0;
    private static byte MONTH_POSITION = 4;
    private static byte DAY_POSITION = 6;
    private static byte DAY_END_POSITION = 8;
    private static byte HOUR_POSITION = 9;
    private static byte HOUR_END_POSITION = 11;
    private static byte MINUTE_POSITION = 12;
    private static byte MINUTE_END_POSITION = 14;
    private static byte SECONDS_POSITION = 15;
    private static byte SECONDS_END_POSITION = 17;
    private static byte NANOS_POSITION = 18;
    private static byte BASE = 10;
    private static byte TIMESTAMP_POSITION = 0;
    private static byte SATELLITE_ID_POSITION = 1;
    private static byte RED_HIGH_LIMIT_POSITION = 2;
    private static byte RED_LOW_LIMIT_POSITION = 5;
    private static byte RAW_VALUE_POSITION = 6;
    private static byte COMPONENT_POSITION = 7;
    private static byte JSON_COMPONENT_POSITION = 0;
    private static byte JSON_TIMESTAMP_POSITION = 1;
    private static byte FILE_ARG_LOCATION = 0;
    private static byte FEATURES_PER_LINE = 8;
    private static byte YEAR_FIELD_SIZE = 8;
    private static byte HOUR_FIELD_SIZE = 2;
    private static byte MINUTE_FIELD_SIZE = 2;
    private static byte SECONDS_FIELD_SIZE = 2;
    private static byte NANOS_FIELD_SIZE = 3;
    private static byte WHITESPACE_COLONS_DOT_SIZE = 4;
    private static int DATE_FIELD_SIZE = YEAR_FIELD_SIZE + HOUR_FIELD_SIZE + MINUTE_FIELD_SIZE + SECONDS_FIELD_SIZE + NANOS_FIELD_SIZE + WHITESPACE_COLONS_DOT_SIZE;
    private static byte RED_LOW_COUNTER = 0;
    private static byte RED_HI_COUNTER = 1;
    private static byte BEFORE_FIRST_HIT = 0;
    private static byte FIRST_HIT = 1;
    private static long INTERVAL_WINDOW = 5L;
    private static String RED_LOW_ALERT = "RED LOW";
    private static String RED_HIGH_ALERT = "RED HIGH";
    private static String SATELLITE_ID_KEY = "satelliteID";
    private static String SEVERITY_KEY = "severity";
    private static String COMPONENT_KEY = "component";
    private static String TIMESTAMP_KEY = "timestamp";
    private static String DOT_PLUS_EXCESS_ZEROS = ".000000";
    private static String JUST_A_DOT = ".";
    private static String JUST_A_Z = "Z";
    private static String DATE_FIELD_REGEX = "^\\d{8} \\d{2}:\\d{2}:\\d{2}.\\d{3}$";
    private static String DELIMITER_REGEX = "\\|";

    private static byte ALERT_TRIGGER_THRESHOLD = 3;

    public static void main(String... args) {

        String line, currDate;
        String[] features;
        int size, satelliteID, redHighLimit, redLowLimit;
        double rawValue;
        byte voltageReadingCount, thermostatReadingCount;
        byte[] readingsCountPair;
        JSONArray outputArray = new JSONArray();
        Pair<Integer, String> satelliteRedLowPair;
        Pair<Integer, String> satelliteRedHighPair;
        Map<Pair<Integer,String>, String[]> intervalMap = new HashMap<>();
        Map<Integer, byte[]> countPerSatelliteMap = new HashMap<>();
        boolean addVoltage, addThermo, checkThermo, checkVoltage;

        LocalDateTime startDateTime = LocalDateTime.MAX, endDateTime = LocalDateTime.MIN, currDateTime;
        Pattern datePattern = Pattern.compile(DATE_FIELD_REGEX);

        if (args.length > 1 || args.length == 0) {
            System.out.println("Please try again with only one command line argument - the path to the input file");
            return;
        }

        try (BufferedReader reader =
                     new BufferedReader(new FileReader(args[FILE_ARG_LOCATION]))) {

            while ((line = reader.readLine()) != null) {
                //Split string based on pipe
                features = line.split(DELIMITER_REGEX);
                //In the real world, invalid lines would be reported to a log file by using log4j, for example
                size = features.length;
                if (size != FEATURES_PER_LINE) {
                    System.out.println(line + " does not contain " + FEATURES_PER_LINE + " features. This line will be skipped because the number of features [" + size + "] is " + ((size > FEATURES_PER_LINE)?" too big.":" too small."));
                    continue;
                }

                //Check format of the datetime with regex check. This code does not verify the date and time at a more fine-grain level but deeper checks can be added.
                currDate = features[TIMESTAMP_POSITION].trim();
                if (currDate.length() != DATE_FIELD_SIZE || !datePattern.matcher(currDate).matches()) {
                    System.out.println(currDate + " is an invalid datetime value. The line " + line + " will be skipped.");
                    continue;
                }

                //Leading zeros need to force numbers to base 10 instead of base 8 (octal)
                try {
                    currDateTime = LocalDateTime.of(Integer.parseInt(currDate.substring(YEAR_POSITION, MONTH_POSITION)),
                                                    Integer.parseInt(currDate.substring(MONTH_POSITION, DAY_POSITION), BASE),
                                                    Integer.parseInt(currDate.substring(DAY_POSITION, DAY_END_POSITION), BASE),
                                                    Integer.parseInt(currDate.substring(HOUR_POSITION, HOUR_END_POSITION), BASE),
                                                    Integer.parseInt(currDate.substring(MINUTE_POSITION, MINUTE_END_POSITION), BASE),
                                                    Integer.parseInt(currDate.substring(SECONDS_POSITION, SECONDS_END_POSITION), BASE),
                                                    Integer.parseInt(currDate.substring(NANOS_POSITION), BASE));
                }
                catch (DateTimeException ex) {
                     System.out.println(currDate + " is an invalid datetime value due to a part of the date being out of range. The line " + line + " will be skipped.");
                     continue;
                }

                //Create a new 5 minute window each time below condtion is reached.
                if (startDateTime.isAfter(endDateTime)) {
                   startDateTime = currDateTime;
                   endDateTime = startDateTime.plusMinutes(INTERVAL_WINDOW);
                   //Below code could not easily be refactored into a method. Will investigate with more time
                   if (!intervalMap.isEmpty()) {
                       intervalMap.entrySet().parallelStream().forEach(e -> {
                          JSONObject currentJSON = new JSONObject()
                                                       .put(SATELLITE_ID_KEY, e.getKey().getKey())
                                                       .put(SEVERITY_KEY, e.getKey().getValue())
                                                       .put(COMPONENT_KEY, e.getValue()[JSON_COMPONENT_POSITION])
                                                       .put(TIMESTAMP_KEY, e.getValue()[JSON_TIMESTAMP_POSITION]);
                           outputArray.put(currentJSON);
                       });
                       System.out.println(outputArray);
                       countPerSatelliteMap.clear();
                       intervalMap.clear();
                   }

                }

                if (currDateTime.isEqual(startDateTime) || (currDateTime.isAfter(startDateTime) &&  currDateTime.isBefore(endDateTime))) {

                    try {
                        satelliteID = Integer.parseInt(features[SATELLITE_ID_POSITION], BASE);
                        redHighLimit = Integer.parseInt(features[RED_HIGH_LIMIT_POSITION], BASE);
                        redLowLimit =  Integer.parseInt(features[RED_LOW_LIMIT_POSITION], BASE);
                        rawValue = Double.parseDouble(features[RAW_VALUE_POSITION]);

                    } catch (NumberFormatException ex) {
                        System.out.println(line + " contains invalid numerical fields. The line will be skipped.");
                        continue;

                    }

                    /*Ingest status telemetry data and create alert messages for the following violation conditions:

                    If for the same satellite there are three battery voltage readings that are under the red low limit within a five minute interval.
                    If for the same satellite there are three thermostat readings that exceed the red high limit within a five minute interval.*/
                    checkVoltage = false;
                    checkThermo = false;

                    if (rawValue < redLowLimit) {

                         if (countPerSatelliteMap.containsKey(satelliteID)) {
                             //First counter keeps tally of threshold breaches against redLow per satellite
                             countPerSatelliteMap.get(satelliteID)[RED_LOW_COUNTER]++;
                             checkVoltage = true;

                         }
                         else countPerSatelliteMap.put(satelliteID, new byte[]{FIRST_HIT, BEFORE_FIRST_HIT});

                    }

                    if (rawValue > redHighLimit) {

                        if (countPerSatelliteMap.containsKey(satelliteID)) {
                            //Second counter keeps tally of threshold breaches against reddHigh per satellite
                            countPerSatelliteMap.get(satelliteID)[RED_HI_COUNTER]++;
                            checkThermo = true;

                        }
                        else countPerSatelliteMap.put(satelliteID, new byte[]{BEFORE_FIRST_HIT, FIRST_HIT});

                    }

                    //Skip lines if voltageReadings & thremostatReadings is already three for the same satelliteID and there is already an entry in the intervalMap for this specific satellite.
                    addVoltage = false;
                    addThermo = false;
                    voltageReadingCount = Byte.MIN_VALUE;
                    thermostatReadingCount = Byte.MIN_VALUE;
                    satelliteRedLowPair = new Pair<Integer, String>(satelliteID, RED_LOW_ALERT);
                    satelliteRedHighPair = new Pair<Integer, String>(satelliteID, RED_HIGH_ALERT);
                    if (countPerSatelliteMap.containsKey(satelliteID)) {
                        readingsCountPair = countPerSatelliteMap.get(satelliteID);
                        voltageReadingCount = readingsCountPair[RED_LOW_COUNTER];
                        thermostatReadingCount = readingsCountPair[RED_HI_COUNTER];
                        if ( voltageReadingCount == ALERT_TRIGGER_THRESHOLD && checkVoltage) {

                            if (!intervalMap.containsKey(satelliteRedLowPair)) addVoltage = true;
                        }
                        if (thermostatReadingCount == ALERT_TRIGGER_THRESHOLD && checkThermo) {

                           if (!intervalMap.containsKey(satelliteRedHighPair)) addThermo = true;
                        }
                    }

                    if (voltageReadingCount == ALERT_TRIGGER_THRESHOLD && addVoltage) {
                        intervalMap.put(satelliteRedLowPair, new String[] {features[COMPONENT_POSITION], currDateTime.toString().replace(DOT_PLUS_EXCESS_ZEROS, JUST_A_DOT) + JUST_A_Z});
                    }
                    if (thermostatReadingCount == ALERT_TRIGGER_THRESHOLD && addThermo) {
                        intervalMap.put(satelliteRedHighPair, new String[] {features[COMPONENT_POSITION], currDateTime.toString().replace(DOT_PLUS_EXCESS_ZEROS, JUST_A_DOT) + JUST_A_Z});
                    }

                }

            }

            //In case there are leftovers in the final 5 minute window. 
            if (!intervalMap.isEmpty()) {
                intervalMap.entrySet().parallelStream().forEach(e -> {
                    JSONObject currentJSON = new JSONObject()
                                                    .put(SATELLITE_ID_KEY, e.getKey().getKey())
                                                    .put(SEVERITY_KEY, e.getKey().getValue())
                                                    .put(COMPONENT_KEY, e.getValue()[JSON_COMPONENT_POSITION])
                                                    .put(TIMESTAMP_KEY, e.getValue()[JSON_TIMESTAMP_POSITION]);
                    outputArray.put(currentJSON);
                });
                System.out.println(outputArray);
            }


        } catch (IOException ex) {
            System.out.println("The file " + args[FILE_ARG_LOCATION] + " could not be found. Please try again.");
            return;
        }
    }
}
