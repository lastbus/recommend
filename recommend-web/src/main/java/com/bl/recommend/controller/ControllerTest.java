package com.bl.recommend.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

/**
 * Created by MK33 on 2016/5/13.
 */
@Controller
public class ControllerTest
{

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String helloWorld()
    {
        return "index";
    }

    @RequestMapping(path = "/{id}", method = RequestMethod.GET)
    public String helloWorld2(@PathVariable String id)
    {
        System.out.println(id);
        return id;
    }

    @RequestMapping(value = "/angularJS")
    @ResponseBody
    public String angularJSTest()
    {
        System.out.println("=======  angularJS test  =======");
        return "{\"test\": \"AngularJS test\"}";
    }



}

class A {
    String name = null;
    int age = 0;

    public A(String name, int age) {
        this.name = name;
        this.age = age;
    }

}
