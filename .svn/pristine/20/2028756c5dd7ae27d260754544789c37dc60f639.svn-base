package com.eurlanda.datashire.server;

import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.springframework.transaction.annotation.Transactional;

@ContextConfiguration({"classpath:config/applicationContext.xml"})
@Transactional(transactionManager = "transactionManager")
@Rollback(true)
public abstract class BaseTest extends AbstractTransactionalJUnit4SpringContextTests {

}