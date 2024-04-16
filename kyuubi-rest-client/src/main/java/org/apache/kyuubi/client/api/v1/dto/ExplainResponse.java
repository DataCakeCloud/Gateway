package org.apache.kyuubi.client.api.v1.dto;

import java.util.Objects;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ExplainResponse {

  private boolean success;
  private String msg;

  public ExplainResponse() {}

  public ExplainResponse(boolean success, String msg) {
    this.success = success;
    this.msg = msg;
  }

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public String getMsg() {
    return msg;
  }

  public void setMsg(String msg) {
    this.msg = msg;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExplainResponse that = (ExplainResponse) o;
    return success == that.success && Objects.equals(msg, that.msg);
  }

  @Override
  public int hashCode() {
    return Objects.hash(success, msg);
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
