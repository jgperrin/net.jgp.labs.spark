package net.jgp.labs.spark.l999_scrapbook.l002;

import java.io.Serializable;
import java.util.Date;

public class Person implements Serializable {
  private static final long serialVersionUID = -5321842705445730009L;

  private String dateOfBirth;
  private Integer name;

  public Person() {
    this.name = 0;
    this.dateOfBirth = null;
  }

  public Person(Integer name, String dateOfBirth) {
    this.name = name;
    this.dateOfBirth = dateOfBirth;
  }

  public String debug() {
    return "" + this.name + "->" + this.dateOfBirth;
  }

  public String getDateOfBirth() {
    return dateOfBirth;
  }

  public Integer getName() {
    return name;
  }

  public void setDateOfBirth(String dateOfBirth) {
    this.dateOfBirth = dateOfBirth;
  }

  public void setName(Integer name) {
    this.name = name;
  }
}