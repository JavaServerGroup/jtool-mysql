package com.jtool.db.dao;

import org.junit.Test;

import com.jtool.db.exception.NotFindRowMapperClassException;

public class NoRowMapperDAOTest {
	
	@Test(expected=NotFindRowMapperClassException.class)
	public void testNoRowMapperDAO() {
		NoRowMapperDAO noRowMapperDAO = new NoRowMapperDAO();
		noRowMapperDAO.init();
	}

}