package com.datastax.driver.core;

import java.lang.management.ManagementFactory;

import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AbstractMBean {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMBean.class);

    private final ObjectName objectName;

    AbstractMBean(ObjectName objectName) {
        this.objectName = objectName;
        register();
    }

    private void register() {
        if (this.objectName == null)
            return;

        try {
            ManagementFactory.getPlatformMBeanServer().registerMBean(this, objectName);
        } catch (Exception e) {
            logger.warn("Error while registering " + objectName, e);
        }
    }

    void unregister() {
        if (this.objectName == null)
            return;

        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(objectName);
        } catch (Exception e) {
            logger.warn("Error while unregistering " + objectName, e);
        }
    }
}
