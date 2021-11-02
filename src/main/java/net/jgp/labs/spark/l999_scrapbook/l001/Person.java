package net.jgp.labs.spark.l999_scrapbook.l001;

import java.io.Serializable;
import java.util.Date;

public class Person implements Serializable {

  private Integer name;
  public Integer getName() {
    return name;
  }

  public void setName(Integer name) {
    this.name = name;
  }

  public Date getDateOfBirth() {
    return dateOfBirth;
  }

  public void setDateOfBirth(Date dateOfBirth) {
    this.dateOfBirth = dateOfBirth;
  }

  private Date dateOfBirth;

  public Person(Integer name, Date dateOfBirth) {
    this.name = name;
    this.dateOfBirth = dateOfBirth;
  }
}