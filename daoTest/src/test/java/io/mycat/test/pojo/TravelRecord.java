package io.mycat.test.pojo;

import java.math.BigDecimal;
import java.util.Date;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author jamie12221
 *  date 2019-05-24 00:59
 **/
@Entity
@Table(name = "travelrecord")
public class TravelRecord {

  @Id
  @Column(name = "id")
  long id;
  @Column(name = "user_id")
  long userId;
  @Column(name = "traveldate")
  Date travelDate;
  @Column(name = "fee")
  BigDecimal fee;
  @Column(name = "days")
  long days;

  public void setId(long id) {
    this.id = id;
  }

  public void setUserId(long userId) {
    this.userId = userId;
  }

  public void setTravelDate(Date travelDate) {
    this.travelDate = travelDate;
  }

  public void setFee(BigDecimal fee) {
    this.fee = fee;
  }

  public void setDays(long days) {
    this.days = days;
  }
}
