package com.yangyh.mr.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @description: 分组比较器
 * @author: yangyh
 * @create: 2019-11-05 20:13
 */
public class WeatherGroupingCompartor extends WritableComparator {

    public WeatherGroupingCompartor() {
        super(WeatherKey.class,true);
    }

    /**
     * 按照年和月分组。返回值为0即同年同月为同一组
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WeatherKey aWeatherKey = (WeatherKey) a;
        WeatherKey bWeatherKey = (WeatherKey) b;

        int result = aWeatherKey.getYear().compareTo(bWeatherKey.getYear());
        if (result == 0) {
            result = aWeatherKey.getMonth().compareTo(bWeatherKey.getMonth());
        }

        return result;

    }
}
