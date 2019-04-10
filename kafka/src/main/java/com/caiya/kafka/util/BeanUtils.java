package com.caiya.kafka.util;

import java.lang.reflect.Field;
import java.util.Arrays;

/**
 * Bean Utils.
 *
 * @author wangnan
 * @since 1.0
 */
public abstract class BeanUtils {


    public static void copyProperties(Object source, Object target, String... ignoreProperties) {
        try {
            Field[] sourceFields = source.getClass().getDeclaredFields();
            for (Field sf : sourceFields) {
                if (Arrays.asList(ignoreProperties).contains(sf.getName())) {
                    continue;
                }
                sf.setAccessible(true);
                Object sv = sf.get(source);
                try {
                    Field tf = target.getClass().getDeclaredField(sf.getName());
                    tf.setAccessible(true);
                    tf.set(target, sv);
                } catch (Exception e) {
                    // ignore
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
