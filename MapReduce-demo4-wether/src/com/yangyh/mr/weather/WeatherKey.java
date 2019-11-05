package com.yangyh.mr.weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @description:
 * @author: yangyh
 * @create: 2019-11-05 19:57
 */
public class WeatherKey  implements WritableComparable<WeatherKey> {
    private Integer year;
    private Integer month;
    private Integer day;
    private Integer temperature;

    public WeatherKey() {
    }

    public WeatherKey(Integer year, Integer month, Integer day, Integer temperature) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.temperature = temperature;
    }

    @Override
    public int compareTo(WeatherKey other) {
        // 默认实现，会被排序比较器覆盖

        int result = this.year.compareTo(other.getYear());

        if (result == 0) {
            result = this.month.compareTo(other.getMonth());

            if (result == 0) {
                result = this.day.compareTo(other.getDay());

                if (result == 0) {
                    result = this.temperature.compareTo(other.getTemperature());
                }
            }
        }
        return result;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(temperature);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        setYear(in.readInt());
        setMonth(in.readInt());
        setDay(in.readInt());
        setTemperature(in.readInt());
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }

    public Integer getDay() {
        return day;
    }

    public void setDay(Integer day) {
        this.day = day;
    }

    public Integer getTemperature() {
        return temperature;
    }

    public void setTemperature(Integer temperature) {
        this.temperature = temperature;
    }
}
