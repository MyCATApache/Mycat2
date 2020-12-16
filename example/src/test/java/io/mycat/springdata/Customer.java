package io.mycat.springdata;

import lombok.*;

import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Getter
@ToString
@AllArgsConstructor
@Table( catalog="db1",name = "customer")
@EqualsAndHashCode
public class Customer extends AbstractEntity {

	String firstname, lastname;

	protected Customer() {
		this.firstname = null;
		this.lastname = null;
	}
}