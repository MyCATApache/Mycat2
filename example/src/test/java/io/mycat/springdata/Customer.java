package io.mycat.springdata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;

import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Getter
@ToString
@AllArgsConstructor
@Table( catalog="db1",name = "sys_user")
public class Customer extends AbstractEntity {

	String firstname, lastname;

	protected Customer() {
		this.firstname = null;
		this.lastname = null;
	}
}