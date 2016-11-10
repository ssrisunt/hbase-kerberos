package com.srisunt.aspect;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

/**
 * Created by ssrisunt on 11/4/16.
 */
@Aspect
public class MyAspect {

    @Around("call(* org.apache.hadoop.security.SecurityUtil.getServerPrincipal(..))")
    public Object ipAroundAdvice(ProceedingJoinPoint proceedingJoinPoint){

        Object value = null;
        try {
            value = proceedingJoinPoint.proceed();
        } catch (Throwable e) {
            e.printStackTrace();
        }

        System.out.println("Before invoking getip() method  value="+value);

        if (value.toString().equals("hbase/song.hortonworks.com@EXAMPLE.COM")) {

            value = "hbase/sandbox.hortonworks.com@EXAMPLE.COM";
            System.out.println("After invoking getip() method. Return value="+value);
        }

        value = value ;//+ " sammy snip.......";
        return value;
    }
}
