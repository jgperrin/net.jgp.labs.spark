package net.jgp.labs.spark.l150_udf.l300_bean;

import java.io.Serializable;
import java.util.Date;

/**
 * A bean that does something silly, but has more than one field.
 * 
 * @author jgp
 */
public class SillyBean implements Serializable {
  private static final long serialVersionUID = 01500201L;

  private String date;
  private Integer value;
  private String valueAsString;

  public SillyBean(Integer x) {
    this.value = x;
    this.valueAsString = "x=" + x;
    this.date = new Date().toString();
  }

  public String getDate() {
    return date;
  }

  public Integer getValue() {
    return value;
  }

  public String getValueAsString() {
    return valueAsString;
  }
  public void setDate(String date) {
    this.date = date;
  }

  public void setValue(Integer value) {
    this.value = value;
  }

  public void setValueAsString(String valueAsSting) {
    this.valueAsString = valueAsSting;
  }

}