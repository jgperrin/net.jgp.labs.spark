package net.jgp.labs.spark.x.model;

import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BookJson {
  private Book b;

  /**
   * @return the b
   */
  public JSONObject getBook() {
    ObjectMapper mapper = new ObjectMapper();
    String bookJson;
    try {
      bookJson = mapper.writeValueAsString(this.b);
    } catch (JsonProcessingException e) {
      bookJson = "N/A";
    }

    JSONObject jo = new JSONObject(bookJson);
    return jo;
  }

  /**
   * @param b
   *          the b to set
   */
  public void setBook(Book b) {
    this.b = b;
  }

}
