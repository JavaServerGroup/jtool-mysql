package com.jtool.db.dao;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.stereotype.Repository;

import com.jtool.db.annotation.DataSource;
import com.jtool.db.annotation.Mapper;
import com.jtool.db.annotation.TableName;

@Repository
@TableName("user")
@DataSource("dataSource")
public class UserDAO extends AbstractDAO {

	@Mapper
	public static final class ObjectRowMapper implements org.springframework.jdbc.core.RowMapper<User> {
		public User mapRow(ResultSet rs, int rowNum) throws SQLException {
			User o = new User();
			o.setId(rs.getInt("id"));
			o.setName(rs.getString("name"));
			o.setAge(rs.getInt("age"));

			return o;
		}
	}
	
	public int[] batchUpdate(final List<User> users) {
		String sql = "insert into " + getTableName() + " (name, age) values(?, ?);";
		int[] updateCounts = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				ps.setString(1, users.get(i).getName());
				ps.setInt(2, users.get(i).getAge());
			}

			public int getBatchSize() {
				return users.size();
			}
		});
		return updateCounts;
	}

}