package com.yangyh.mr.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @description: 自定义排序比较器
 * @author: yangyh
 * @create: 2019-11-05 20:18
 */
public class WeatherSortCompartor extends WritableComparator {

    public WeatherSortCompartor() {
        super(WeatherKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WeatherKey aWeatherKey = (WeatherKey) a;
        WeatherKey bWeatherKey = (WeatherKey) b;

        // 同年同月的温度倒序排列
        int result = aWeatherKey.getYear().compareTo(bWeatherKey.getYear());
        if (result == 0) {
            result = aWeatherKey.getMonth().compareTo(bWeatherKey.getMonth());
            if (result == 0) {
                // 天气倒序
                result = bWeatherKey.getTemperature().compareTo(aWeatherKey.getTemperature());
            }
        }

        return result;
    }
}
