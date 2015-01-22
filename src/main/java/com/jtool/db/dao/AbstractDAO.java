package com.jtool.db.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import com.jtool.db.annotation.Mapper;
import com.jtool.db.annotation.TableName;
import com.jtool.db.exception.NewRowMapperInstanceException;
import com.jtool.db.exception.NotFindRowMapperClassException;

public abstract class AbstractDAO implements ApplicationContextAware {

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	protected JdbcTemplate jdbcTemplate;
	protected SimpleJdbcInsert simpleJdbcInsert;

	protected DataSource dataSource;

	protected String tableName;

	protected Class<RowMapper<?>> rowMapper;

	private ApplicationContext context;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		context = applicationContext;
	}

	@PostConstruct
	protected void init() {
		initTableName();
		initRowMapper();
		initDataSource();

		this.jdbcTemplate = new JdbcTemplate(dataSource);
		this.simpleJdbcInsert = new SimpleJdbcInsert(dataSource).withTableName(tableName).usingGeneratedKeyColumns("id");
	}

	private void initDataSource() {
		Class<?> clazz = this.getClass();
		String dataSourceString = clazz.getAnnotation(com.jtool.db.annotation.DataSource.class).value();
		dataSource = context.getBean(dataSourceString, DataSource.class);
	}

	private void initRowMapper() {
		Class<?> clazz = this.getClass();
		Class<?>[] clazzes = clazz.getDeclaredClasses();
		rowMapper = findRowMapperClass(clazzes);
	}

	@SuppressWarnings("unchecked")
	private Class<RowMapper<?>> findRowMapperClass(Class<?>[] clazzes) {
		
		Optional<Class<?>> c = Arrays.asList(clazzes).stream().filter(e -> e.getAnnotation(Mapper.class) != null).findFirst();
		
		if(c.isPresent()) {
			return (Class<RowMapper<?>>) c.get();
		} else {
			throw new NotFindRowMapperClassException();
		}
		
	}

	protected RowMapper<?> makeRowMapperInstance() {
		try {
			return rowMapper.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new NewRowMapperInstanceException();
		}
	}

	private void initTableName() {
		Class<?> clazz = this.getClass();
		tableName = clazz.getAnnotation(TableName.class).value();
	}

	protected <T> List<T> selectAll() {
		String selectSQL = "select * from " + tableName;
		logger.debug("准备查找全部数据：" + selectSQL);
		@SuppressWarnings("unchecked")
		List<T> result = (List<T>) jdbcTemplate.query(selectSQL, makeRowMapperInstance());
		logger.debug("查找全部数据的条数：" + result.size());
		return result;
	};

	protected int add(Object object) {
		SqlParameterSource sps = new BeanPropertySqlParameterSource(object);
		logger.debug("准备插入对象：" + object);
		int id = simpleJdbcInsert.executeAndReturnKey(sps).intValue();
		logger.debug("插入成功:" + object);
		return id;
	}

	protected <T> Optional<T> selectById(Object id) {
		try {
			String selectByIdSQL = "select * from " + tableName + " where id = ?";
			logger.debug("准备根据ID查找：" + selectByIdSQL + "\t" + id.toString());
			@SuppressWarnings("unchecked")
			T t = (T) jdbcTemplate.queryForObject(selectByIdSQL, makeRowMapperInstance(), id.toString());
			logger.debug("根据ID查找到：" + t);
			return Optional.of(t);
		} catch (EmptyResultDataAccessException e) {
			logger.debug("根据ID(" + id + ")查找不到对象");
			return Optional.empty();
		}
	};

	protected int countTotal() {
		return countTotalFilterBy("");
	}

	protected int countTotalFilterBy(String filter, Object... args) {
		String sql = "select count(1) from " + tableName + " " + filter;
		logger.debug("准备计算记录条数：" + sql + "\t" + Arrays.toString(args));
		int result = jdbcTemplate.queryForObject(sql, args, Integer.class);
		logger.debug("计算记录条数为：" + result);
		return result;
	}

	protected int deleteById(Object id) {
		String sql = "delete from " + tableName + " where id = ?";
		logger.debug("准备根据ID删除记录：" + sql + "\t" + id);
		int i = jdbcTemplate.update(sql, id);
		logger.debug("删除记录条数：" + i);
		return i;
	}

	protected int deleteBy(String filterStr, Object... args) {
		String sql = "delete from " + tableName + " " + filterStr;
		logger.debug("准备删除数据：" + sql + "\t" + Arrays.toString(args));
		int i = jdbcTemplate.update(sql, args);
		logger.debug("删除记录条数：" + i);
		return i;
	}

	protected <T> Optional<T> selectFilterByAsSingle(String filterStr, Object... args) {
		String sql = "select * from " + tableName + " " + filterStr;
		logger.debug("准备查找数据：" + sql + "\t" + Arrays.toString(args));
		try {
			@SuppressWarnings("unchecked")
			T t = (T) jdbcTemplate.queryForObject(sql, makeRowMapperInstance(), args);
			logger.debug("查找到记录：" + t);
			return Optional.of(t);
		} catch (EmptyResultDataAccessException e) {
			logger.debug("没有查找到数据");
			return Optional.empty();
		}
	}

	protected <T> List<T> selectFilterByAsList(String filterStr, Object... args) {
		String sql = "select * from " + tableName + " " + filterStr;
		logger.debug("准备查找数据：" + sql + "\t" + Arrays.toString(args));
		@SuppressWarnings("unchecked")
		List<T> result = (List<T>) jdbcTemplate.query(sql, makeRowMapperInstance(), args);
		logger.debug("查找到符合条件记录条数：" + result.size());
		return result;
	}

	protected List<Map<String, Object>> selectFilterByAsRows(String filterStr, Object... args) {
		String sql = "select * from " + tableName + " " + filterStr;
		logger.debug("准备查找数据：" + sql + "\t" + Arrays.toString(args));
		List<Map<String, Object>> result = jdbcTemplate.queryForList(sql, args);
		logger.debug("查找到符合条件记录条数：" + result.size());
		return result;
	}

	protected List<Map<String, Object>> selectByStartAndLimit(Integer start, Integer limit) {
		return selectByStartAndLimitOrderBy(start, limit, "");
	}

	protected List<Map<String, Object>> selectByStartAndLimitOrderBy(Integer start, Integer limit, String orderBy) {
		return selectFilterByStartAndLimitOrderBy("", start, limit, orderBy);
	}

	protected List<Map<String, Object>> selectFilterByStartAndLimitOrderBy(String filterBy, Integer start, Integer limit, String orderBy, Object... args) {
		String sql = "select * from " + tableName + " " + filterBy + " " + orderBy + " limit ?, ?";

		Object[] argsWithEndStart = makeArgsWithStartAndLimit(start, limit, args);

		logger.debug("准备查找数据：" + sql + "\t" + Arrays.toString(argsWithEndStart));
		List<Map<String, Object>> result = jdbcTemplate.queryForList(sql, argsWithEndStart);
		logger.debug("查找到符合条件记录条数：" + result.size());
		return result;
	}

	private Object[] makeArgsWithStartAndLimit(Integer start, Integer limit, Object... args) {
		List<Object> params = new ArrayList<Object>();
		for(Object arg : args) {
			params.add(arg);
		}
		params.add(start);
		params.add(limit);
		return params.toArray();
	}

	protected <T> List<T> selectFilterByStartAndLimitOrderByAsList(String filterBy, Integer start, Integer limit, String orderBy, Object... args) {
		String sql = "select * from " + tableName + " " + filterBy + " " + orderBy + " limit ?, ?";

		Object[] argsWithEndStart = makeArgsWithStartAndLimit(start, limit, args);
		logger.debug("准备查找数据：" + sql + "\t" + Arrays.toString(argsWithEndStart));
		@SuppressWarnings("unchecked")
		List<T> result = (List<T>) jdbcTemplate.query(sql, makeRowMapperInstance(), argsWithEndStart);
		logger.debug("查找到符合条件记录条数：" + result.size());
		return result;
	}

	protected List<Map<String, Object>> execSelectSql(String sql, Object... args) {
		logger.debug("准备查找数据：" + sql + "\t" + Arrays.toString(args));
		List<Map<String, Object>> result = jdbcTemplate.queryForList(sql, args);
		logger.debug("查找到符合条件记录条数：" + result.size());
		return result;
	}

	protected <T> T execSelectSqlAsObject(String sql, Object... args) {
		logger.debug("准备查找数据：" + sql + "\t" + Arrays.toString(args));
		@SuppressWarnings("unchecked")
		T t = (T) jdbcTemplate.queryForObject(sql, makeRowMapperInstance(), args);
		logger.debug("查找到符合条件记录：" + t);
		return t;
	}

	@SuppressWarnings("unchecked")
	protected <T> List<T> execSqlAsObjectList(String sql, Object... args) {
		logger.debug("准备查找数据：" + sql + "\t" + Arrays.toString(args));
		List<T> result = (List<T>) jdbcTemplate.query(sql, makeRowMapperInstance(), args);
		logger.debug("查找到符合条件记录条数：" + result.size());
		return result;
	}

	protected int execUpdate(String sql, Object... args) {
		logger.debug("执行修改操作：" + sql + "\t" + Arrays.toString(args));
		int result = jdbcTemplate.update(sql, args);
		logger.debug("执行修改操作条数：" + result);
		return result;
	}

	public String getTableName() {
		return tableName;
	}
	
	protected boolean hasOnlyOneRecord(String where, Object... args) {
		return 1  == this.countTotalFilterBy(where, args);
	}
	
	protected boolean hasRecord(String filter, Object... args) {
		String sql = "select 1 from " + tableName + " " + filter + " limit 0, 1";
		logger.debug(sql + "\t" + Arrays.toString(args));
		return jdbcTemplate.queryForList(sql, args).size() > 0;
	}

}
