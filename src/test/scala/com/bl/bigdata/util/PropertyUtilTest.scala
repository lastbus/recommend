package com.bl.bigdata.util

import org.junit.Test

/**
  * Created by blemall on 3/27/16.
  */
@Test
class PropertyUtilTest {

    @Test
    def testPropertyUtil = {
        val b = PropertyUtil.get("hello")
        assert("world" == b)
    }

    @Test
    def testNonExistProperty = {
        val b = PropertyUtil.get("hello1")
        assert(b == null)
    }
}
