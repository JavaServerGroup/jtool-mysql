package com.jtool.db.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Resource;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;

@ContextConfiguration(locations = "/testDB-config.xml")
public class UserDAOTest extends AbstractTransactionalJUnit4SpringContextTests {
	
	@Resource
	private UserDAO userDAO;
	
	@Test
	public void testSelectAll() {
		List<User> users = new ArrayList<>();
		users.add(genUserPojo(1, "jialechan", 8));
		users.add(genUserPojo(2, "KKL", 18));
		users.add(genUserPojo(3, "Ken", 28));
		
		List<User> result = userDAO.selectAll();
		
		Assert.assertEquals(users, result);
	}
	
	@Test
	public void testSelectAllWithNoData() {
		int deleted = userDAO.deleteBy("");
		Assert.assertEquals(3, deleted);
		
		List<User> users = userDAO.selectAll();
		Assert.assertEquals(0, users.size());
	}
	
	@Test
	public void testSelectById() {
		
		User user = genUserPojo(1, "jialechan", 8);
		
		Optional<User> userFromDB = userDAO.selectById(1);
		
		Assert.assertTrue(userFromDB.isPresent());
		Assert.assertEquals(user, userFromDB.get());
	}
	
	@Test
	public void testCountTotal() {
		Assert.assertEquals(3, userDAO.countTotal());
	}
	
	@Test
	public void testAdd() {
		User user = new User();
		user.setAge(1);
		user.setName("Tim");
		
		int id = userDAO.add(user);
		
		user.setId(id);
		
		Optional<User> userFromDb = userDAO.selectById(id);
		
		Assert.assertTrue(userFromDb.isPresent());
		
		Assert.assertEquals(user, userFromDb.get());
	}
	
	@Test
	public void testSelectFilterBy() {
		User user = genUserPojo(1, "jialechan", 8);
		
		Optional<User> userFromDB = userDAO.selectFilterByAsSingle("where name = ?" , "jialechan");
		
		Assert.assertTrue(userFromDB.isPresent());
		Assert.assertEquals(user, userFromDB.get());
	}
	
	@Test
	public void testSelectFilterByWithNoData() {
		Optional<User> userFromDB = userDAO.selectFilterByAsSingle("where name = ?" , "nobody");
		Assert.assertFalse(userFromDB.isPresent());
	}
	
	@Test(expected=IncorrectResultSizeDataAccessException.class)
	public void testSelectFilterByWithMoreThanOneResult() {
		userDAO.selectFilterByAsSingle("where age > ?", 1);
	}
	
	@Test
	public void testSelectFilterByAsList() {
		List<User> users = new ArrayList<>();
		users.add(genUserPojo(1, "jialechan", 8));
		users.add(genUserPojo(2, "KKL", 18));
		
		List<User> userFromDB = userDAO.selectFilterByAsList("where age < ?", 20);
		
		Assert.assertEquals(users, userFromDB);
	}
	
	@Test
	public void testSelectFilterByAsListWithNoData() {
		List<User> userFromDB = userDAO.selectFilterByAsList("where age < ?", 0);
		Assert.assertEquals(0, userFromDB.size());
	}
	
	@Test
	public void testSelectFilterByAsRows() {
		Map<String, Object> user1 = genUserMap(1, "jialechan", 8);
		Map<String, Object> user2 = genUserMap(2, "KKL", 18); 
		
		List<Map<String, Object>> userFromDB = userDAO.selectFilterByAsRows("where age < ?", 20);
		
		Assert.assertEquals(2, userFromDB.size());
		Assert.assertTrue(userFromDB.contains(user1));
		Assert.assertTrue(userFromDB.contains(user2));
	}
	
	@Test
	public void testSelectFilterByAsRowsWithNoData() {
		List<Map<String, Object>> userFromDB = userDAO.selectFilterByAsRows("where age < ?", 0);
		Assert.assertEquals(0, userFromDB.size());
	}
	
	@Test
	public void testSelectFilterByStartAndLimitOrderBy() {
		Map<String, Object> user2 = genUserMap(2, "KKL", 18); 
		
		List<Map<String, Object>> userFromDB = userDAO.selectFilterByStartAndLimitOrderBy("where age < ?", 0, 1, "order by id desc", 20);
		
		Assert.assertEquals(1, userFromDB.size());
		Assert.assertTrue(userFromDB.contains(user2));
	}
	
	@Test
	public void testSelectByStartAndLimit() {
		Map<String, Object> user = genUserMap(1, "jialechan", 8); 
		
		List<Map<String, Object>> userFromDB = userDAO.selectByStartAndLimit(0, 1);
		
		Assert.assertEquals(1, userFromDB.size());
		Assert.assertTrue(userFromDB.contains(user));
	}
	
	@Test
	public void testSelectByStartAndLimitOrderBy() {
		Map<String, Object> user = genUserMap(3, "Ken", 28); 
		
		List<Map<String, Object>> userFromDB = userDAO.selectByStartAndLimitOrderBy(0, 1, "order by id desc");
		
		Assert.assertEquals(1, userFromDB.size());
		Assert.assertTrue(userFromDB.contains(user));
	}
	
	@Test
	public void testSelectFilterByStartAndLimitOrderByWithNoData() {
		List<Map<String, Object>> userFromDB = userDAO.selectFilterByStartAndLimitOrderBy("where age > ?", 0, 100, "order by age desc", 100);
		Assert.assertEquals(0, userFromDB.size());
	}
	
	@Test
	public void testSelectFilterByStartAndLimitOrderByAsList() {
		User user = genUserPojo(2, "KKL", 18); 
		
		List<User> userFromDB = userDAO.selectFilterByStartAndLimitOrderByAsList("where age < ?", 0, 1, "order by id desc", 20);
		
		Assert.assertEquals(1, userFromDB.size());
		Assert.assertTrue(userFromDB.contains(user));
	}
	
	@Test
	public void testSelectFilterByStartAndLimitOrderByAsListWithNoData() {
		List<User> userFromDB = userDAO.selectFilterByStartAndLimitOrderByAsList("where age < ?", 0, 1, "order by id desc", 0);
		Assert.assertEquals(0, userFromDB.size());
	}
	
	@Test
	public void testExecSelectSql() {
		Map<String, Object> user2 = genUserMap(2, "KKL", 18); 
		
		String sql = "select * from " + userDAO.getTableName() + " where name = ?";
		
		List<Map<String, Object>> userFromDB = userDAO.execSelectSql(sql, "KKL");
		
		Assert.assertEquals(1, userFromDB.size());
		Assert.assertTrue(userFromDB.contains(user2));
	}
	
	@Test
	public void testExecSelectSqlNotExit() {
		String sql = "select * from " + userDAO.getTableName() + " where name = ?";
		List<Map<String, Object>> userFromDB = userDAO.execSelectSql(sql, "KKL2");
		Assert.assertEquals(0, userFromDB.size());
	}
	
	@Test
	public void testExecUpdate() {
		User user = genUserPojo(1, "jialechan", 8);
		
		String sql = "update " + userDAO.getTableName() + " set name = ? where id = ?";
		
		int updated = userDAO.execUpdate(sql, "KKL2", 1);
		
		Assert.assertEquals(1, updated);
		
		user.setName("KKL2");
		
		Assert.assertEquals(user, userDAO.selectById(1).get());
	}
	
	@Test
	public void testExecUpdateButNotMatch() {
		String sql = "update " + userDAO.getTableName() + " set name = 'nomatch' where id = -1";
		int updated = userDAO.execUpdate(sql);
		
		Assert.assertEquals(0, updated);
	}
	
	@Test
	public void testExecSelectSqlAsObject() {
		User user = genUserPojo(1, "jialechan", 8);
		
		String sql = "select * from " + userDAO.getTableName() + " where name = ?";
		
		User userFromDB = userDAO.execSelectSqlAsObject(sql, "jialechan");
		
		Assert.assertEquals(user, userFromDB);
	}
	
	@Test
	public void testExecSqlAsObjectList() {
		List<User> users = new ArrayList<>();
		users.add(genUserPojo(1, "jialechan", 8));
		users.add(genUserPojo(2, "KKL", 18));
		
		String sql = "select * from " + userDAO.getTableName() + " where age < ?";
		
		List<User> userFromDB = userDAO.execSqlAsObjectList(sql, 20);
		
		Assert.assertEquals(users, userFromDB);
	}
	
	@Test
	public void testDeleteById() {
		int deleted = userDAO.deleteById(1);
		Assert.assertEquals(1, deleted);
		
		Optional<User> shouldBeEmpty = userDAO.selectById(1);
		Assert.assertFalse(shouldBeEmpty.isPresent());
	}
	
	@Test
	public void testDeleteBy() {
		int deleted = userDAO.deleteBy("where age > ?", 1);
		Assert.assertEquals(3, deleted);
		
		List<User> users = userDAO.selectFilterByAsList("where age > ?", 1);
		Assert.assertEquals(0, users.size());
	}
	
	@Test
	public void testHasOnlyOneRecord() {
		Assert.assertTrue(userDAO.hasOnlyOneRecord("where name = ?", "Ken"));
		Assert.assertFalse(userDAO.hasOnlyOneRecord("where name != ?", "Ken"));
	}
	
	@Test
	public void testHasRecord() {
		Assert.assertTrue(userDAO.hasRecord("where name = ?", "Ken"));
		Assert.assertFalse(userDAO.hasRecord("where name = ?", "nobody"));
	}
	
	private User genUserPojo(int id, String name, int age) {
		User user = new User();
		user.setAge(age);
		user.setName(name);
		user.setId(id);
		return user;
	}
	
	private Map<String, Object> genUserMap(int id, String name, int age) {
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("age", age);
		result.put("id", id);
		result.put("name", name);
		
		return result;
	}


}