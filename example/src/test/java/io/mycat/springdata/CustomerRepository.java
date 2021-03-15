package io.mycat.springdata;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;
import org.springframework.scheduling.annotation.Async;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.springframework.transaction.annotation.Transactional;

/**
 * Repository to manage {@link Customer} instances.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public interface CustomerRepository extends JpaRepository<Customer, Long> {

	/**
	 * Special customization of {@link CrudRepository#findOne(java.io.Serializable)} to return a JDK 8 {@link Optional}.
	 *
	 * @param id
	 * @return
	 */
	Optional<Customer> findById(Long id);

	/**
	 * Saves the given {@link Customer}.
	 *
	 * @param customer
	 * @return
	 */
	<S extends Customer> S save(S customer);

	/**
	 * Sample method to derive a query from using JDK 8's {@link Optional} as return type.
	 *
	 * @param lastname
	 * @return
	 */
	Optional<Customer> findByLastname(String lastname);

	/**
	 * Sample default method to show JDK 8 feature support.
	 *
	 * @param customer
	 * @return
	 */
	default Optional<Customer> findByLastname(Customer customer) {
		return findByLastname(customer == null ? null : customer.lastname);
	}

	/**
	 * Sample method to demonstrate support for {@link Stream} as a return type with a custom query. The query is executed
	 * in a streaming fashion which means that the method returns as soon as the first results are ready.
	 *
	 * @return
	 */
	@Query("select c from Customer c")
	Stream<Customer> streamAllCustomers();

	/**
	 * Sample method to demonstrate support for {@link Stream} as a return type with a derived query. The query is
	 * executed in a streaming fashion which means that the method returns as soon as the first results are ready.
	 *
	 * @return
	 */
	Stream<Customer> findAllByLastnameIsNotNull();

	@Async
	CompletableFuture<List<Customer>> readAllBy();
	@Modifying
	@Transactional(rollbackFor = Exception.class)
	@Query("update Customer c set c.firstname= :firstName where id = :id")
	Integer updateFirstNameById(@Param("id") Long id ,@Param("firstName") String firstName);

	@Modifying
	@Transactional(rollbackFor = Exception.class)
	@Query(value = "insert into db1.customer(id, firstname) values (:id, :firstName)",nativeQuery = true)
	Integer insert(@Param("id")Long id, @Param("firstName") String firstName);

}
