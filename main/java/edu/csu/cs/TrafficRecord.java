package edu.csu.cs;

public class TrafficRecord {

    String REPORT_ID;
    String TIMESTAMP;

    String avgSpeed;
    String vehicleCount;


    public TrafficRecord(String REPORT_ID, String TIMESTAMP, String avgSpeed, String vehicleCount) {
        this.REPORT_ID = REPORT_ID;
        String info[] = TIMESTAMP.split("T")[1].split(":");
        this.TIMESTAMP = info[0]+ "00";
        this.avgSpeed = avgSpeed;
        this.vehicleCount = vehicleCount;
    }
}
