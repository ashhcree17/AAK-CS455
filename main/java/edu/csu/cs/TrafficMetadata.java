package edu.csu.cs;

public class TrafficMetadata {

    String POINT_1_STREET;
    //String DURATION_IN_SEC;
    //String POINT_1_NAME;
    String POINT_1_CITY;
   // String POINT_2_NAME;
    //String POINT_2_LNG;
    String POINT_2_STREET;
    //String NDT_IN_KMH;
    //String POINT_2_POSTAL_CODE;
    String POINT_2_COUNTRY;
    //String POINT_1_STREET_NUMBER;
    //String ORGANISATION;
    //String POINT_1_LAT;
    //String POINT_2_LAT;
    //String POINT_1_POSTAL_CODE;
    //String POINT_2_STREET_NUMBER;
    String POINT_2_CITY;
    //String extID;
    String ROAD_TYPE;
    //String POINT_1_LNG;
    String REPORT_ID;
    String POINT_1_COUNTRY;
    String DISTANCE_IN_METERS;
    //String REPORT_NAME;
    //String RBA_ID;
    //String _id;


    public TrafficMetadata(String POINT_1_STREET, String POINT_1_CITY, String POINT_2_STREET, String POINT_2_COUNTRY, String POINT_2_CITY, String ROAD_TYPE, String REPORT_ID, String POINT_1_COUNTRY, String DISTANCE_IN_METERS) {
        this.POINT_1_STREET = POINT_1_STREET;
        this.POINT_1_CITY = POINT_1_CITY;
        this.POINT_2_STREET = POINT_2_STREET;
        this.POINT_2_COUNTRY = POINT_2_COUNTRY;
        this.POINT_2_CITY = POINT_2_CITY;
        this.ROAD_TYPE = ROAD_TYPE;
        this.REPORT_ID = REPORT_ID;
        this.POINT_1_COUNTRY = POINT_1_COUNTRY;
        this.DISTANCE_IN_METERS = DISTANCE_IN_METERS;
    }
}
