package com.example.springbatch2demo.batchscheduler.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class MyDateUtils {

    public static boolean isValidPastDate(String dateString) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setLenient(false);
        try {
            String[] dateParts = dateString.split("-");
            if (dateParts.length != 3) {
                return false;
            }
            int year = Integer.parseInt(dateParts[0]);
            int month = Integer.parseInt(dateParts[1]);
            int day = 1;
            if (!dateParts[2].equals("XX")) {
                day = Integer.parseInt(dateParts[2]);
            } else {
                Calendar calendar = Calendar.getInstance();
                calendar.set(Calendar.YEAR, year);
                calendar.set(Calendar.MONTH, month - 1);
                day = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
            }
            Calendar calendar = Calendar.getInstance();
            calendar.setLenient(false);
            calendar.set(Calendar.YEAR, year);
            calendar.set(Calendar.MONTH, month - 1);
            calendar.set(Calendar.DAY_OF_MONTH, day);
            Date date = calendar.getTime();
            Date currentDate = new Date();
            return date.before(currentDate);
        } catch (NumberFormatException | ParseException e) {
            return false;
        }
    }
}