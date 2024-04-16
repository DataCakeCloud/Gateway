package org.apache.kyuubi.client.api.v1.dto;

import java.util.Objects;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ExplainRequest {

  private String tenant;
  private String catalog;
  private String engineType;
  private String database = "default";
  private String sql;

  public ExplainRequest() {}

  public ExplainRequest(
      String tenant, String catalog, String engineType, String database, String sql) {
    this.tenant = tenant;
    this.catalog = catalog;
    this.engineType = engineType;
    this.database = database;
    this.sql = sql;
  }

  public String getTenant() {
    return tenant;
  }

  public void setTenant(String tenant) {
    this.tenant = tenant;
  }

  public String getCatalog() {
    return catalog;
  }

  public void setCatalog(String catalog) {
    this.catalog = catalog;
  }

  public String getEngineType() {
    return engineType;
  }

  public void setEngineType(String engineType) {
    this.engineType = engineType;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExplainRequest that = (ExplainRequest) o;
    return Objects.equals(tenant, that.tenant)
        && Objects.equals(catalog, that.catalog)
        && Objects.equals(engineType, that.engineType)
        && Objects.equals(database, that.database)
        && Objects.equals(sql, that.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenant, catalog, engineType, database, sql);
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
